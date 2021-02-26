using System;
using RelaNet.Messages;
using RelaNet.Sockets;
using RelaNet.Utilities;
using RelaStructures;

namespace RelaNet.Snapshots
{
    public class NetExecutorSnapper : INetExecutor
    {
        /*
         * 
         * Theory
         * maintain own Sents per player
         * start each Sent with a header message that contains a sequence number
         * and a timestamp
         * track each entity snapshot under each sequence number and the timestamp
         * -> no, just use sequence # to reject old packets
         * 
         * each header includes a scene number, if it does not match, the message is thrown out
         * 
         * An overview of processing snap messages:
         * - each packet is affixed with a header that specifies its timestamp
         * - when a client connects, the server tells it what timestamp is current (TC)
         * - whenever new packets are received, they are rejected unless they are
         *   between TC-15 and TC+16 (we store 32 snaps at once)
         * - the client adjusts its current timestamp independently of the server
         * - because these are computed on delta time, they shouldn't get out of sync
         *   even if the server or client has a hiccup
         *   (NOTE: when tracking time elapsed, make sure to SUBTRACT ticktime rather
         *    than setting to 0 once ticktime is hit)
         * 
         */

        // what do the actual messages look like?
        // EventPacketHeader 
        // [ushort - timestamp] [byte - snapper sequence number] [byte - scene sequence number]

        // EventGhostFirstClass
        // [byte - entityid] [byte - entitytype] [byte - length (N)] [NxBytes - full pack]
        // EventGhostSecondClass
        // [ushort - entityid] [byte - entitytype] [byte - length (N)] [NxBytes - full pack]

        // EventDeltaFirstClass
        // [byte - entityid] [ushort - basis timestamp] [byte - length (N)] [NxBytes - delta]
        // EventDeltaSecondClass
        // [ushort - entityid] [ushort - basis timestamp] [byte - length (N)] [NxBytes - delta]

        // Deghost: the entity phases out of existence. The server will not send new snapshots
        //          until the entity is reghosted for us.

        // EventDeghostFirstClass
        // [byte - entityid]
        // EventDeghostSecondClass
        // [ushort - entityid]  

        // Destruct: we can delete the entity and its history. Unless the entire scene is
        //           being destroyed, should only happen after an entity is deghosted for 
        //           a significant amount of time.

        // EventDestructFirstClass
        // [byte - entityid]
        // EventDestructSecondClass
        // [ushort - entityid]

        // SnapSettings: sent when player first joins the server

        // EventSnapSettings
        // [ushort - starting timestamp] [ushort - starting sequence number] [byte - starting scene]

        // NewScene: tells client to throw out all existing entities

        // EventNewScene
        // [byte - scene number]

        // Resend: If we receive a delta that cannot be processed, we ask the server
        //         to resend a full pack (ghost) for that entity

        // EventResendFirstClass
        // [byte - entityid] [ushort - timestamp]
        // EventResendSecondClass
        // [ushort - entityid] [ushort - timestamp]

        public ushort EventPacketHeader = 0;
        public ushort EventGhostFirstClass = 1;
        public ushort EventGhostSecondClass = 2;
        public ushort EventDeltaFirstClass = 3;
        public ushort EventDeltaSecondClass = 4;
        public ushort EventDeghostFirstClass = 5;
        public ushort EventDeghostSecondClass = 6;
        public ushort EventDestructFirstClass = 7;
        public ushort EventDestructSecondClass = 8;
        public ushort EventSnapSettings = 9;
        public ushort EventNewScene = 10;
        public ushort EventTickSettings = 11;

        // client events
        public ushort EventClientInput = 12;
        public ushort EventResendFirstClass = 13;
        public ushort EventResendSecondClass = 14;

        public const int PacketHeaderLength = 6;
        public const int SentSizeWithHeader = Sent.EmptySizeWithAck + PacketHeaderLength;

        public NetServer Server;

        public bool Active { get; private set; } = false;

        private Sent[] PlayerSents = new Sent[4];
        private float[] TimeSincePlayerSents = new float[4];
        public float TimeSincePlayerSentMax = 20; // send 50 times a second by default

        public int SentMaxRetries = 20;
        public float SentRetryTimer = 20;

        private ReArrayIdPool<SnapHandshake>[] PlayerHandshakes = null;
        private SnapHandshake[] CurrentHandshakes = null;


        private ISnapSimulator Simulator = null;
        private bool ResimulateRequested = false;
        private ushort ResimulateStartTimestamp = 0;
        public ushort SimulateTimestamp { get; private set; } = 0;



        private ISnapper[] Snappers = new ISnapper[8];
        private int SnapperCount = 0;
        private short[] EntityIdToSnapperIdFirstClass = new short[byte.MaxValue + 1];
        private short[] EntityIdToSnapperIdSecondClass = new short[8];

        private byte NextEntityIdFirstClass = 0;
        private ushort NextEntityIdSecondClass = 0;

        private ushort[][] BasisTimestampsFirstClass = new ushort[8][];
        private bool[][] AnyBasisFirstClass = new bool[8][];
        private ushort[][] BasisTimestampsSecondClass = new ushort[8][];
        private bool[][] AnyBasisSecondClass = new bool[8][];

        // current scene-- scene changes indicate all entities are thrown out
        private byte SceneNumber = 0;
        // current time is the current timestamp
        public ushort CurrentTime { get; private set; } = 0;
        // the seq. no. increments each time the CurrentTime rolls over back to 0
        // we reject packets whose headers are more than +/-1 from our current 
        // sequence number
        private byte SequenceNumber = 0;
        // TickMS is how many ms have elapsed since the last timestamp update
        private float TickMS = 0;
        public float TickMSTarget { get; private set; } = 20;


        private byte SceneChangeType = 0;
        private ushort SceneChangeCustomId = 0;
        private string SceneChangeTextA = "";
        private string SceneChangeTextB = "";
   

        // client time is the current timestamp on the client end
        // the client is [ClientTickMSAdjustment] ms behind the server
        // so its timestamp and elapsed tickms is different from the server.
        // remember: ClientTime is behind ServerTime
        public ushort ClientTime { get; private set; } = 0;
        public byte ClientSequenceNumber { get; private set; } = 0;
        public float ClientTickMS { get; private set; } = 0;
        public float ClientTickMSAdjustment = 100;


        public ushort ClientInputTime { get; private set; } = 0;
        public byte ClientInputSequenceNumber { get; private set; } = 0;
        public float ClientInputTickMS { get; private set; } = 0;
        // ushort version of the tick ms, for transmission with input commands
        private ushort ClientInputTickMSushort = 0;
        public float ClientInputTickMSInputOffset { get; private set; } = 0;

        // receiving values:
        // these values are stored each time we hit a packet header
        private byte ReceiveSceneNumber = 0;
        private ushort ReceiveCurrentTime = 0;
        private byte ReceiveSequenceNumber = 0;
        private bool ReceiveValid = false;


        // input methods
        private Sent ClientInputSent = null;
        private float ClientInputSentTickMS = 0;
        private ISnapInputManager[] InputManagers = new ISnapInputManager[4];
        private int InputManagerCount = 0;


        // callbacks
        // (changeType, customId, texta, textb)
        public Action<byte, ushort, string, string> CallbackClientNewScene;

        // Constructor
        public NetExecutorSnapper()
        {
            // fill entity map with -1s
            for (int i = 0; i < EntityIdToSnapperIdFirstClass.Length; i++)
                EntityIdToSnapperIdFirstClass[i] = -1;

            for (int i = 0; i < EntityIdToSnapperIdSecondClass.Length; i++)
                EntityIdToSnapperIdSecondClass[i] = -1;
        }



        // Interface Methods
        #region NetExec Interface Methods
        public ushort Register(NetServer server, ushort startIndex)
        {
            Server = server;
            
            if (Server.IsHost)
            {
                SceneNumber = 1;

                for (int i = 0; i < BasisTimestampsFirstClass.Length; i++)
                    BasisTimestampsFirstClass[i] = new ushort[byte.MaxValue + 1];

                for (int i = 0; i < AnyBasisFirstClass.Length; i++)
                    AnyBasisFirstClass[i] = new bool[byte.MaxValue + 1];

                for (int i = 0; i < BasisTimestampsSecondClass.Length; i++)
                    BasisTimestampsSecondClass[i] = new ushort[8];

                for (int i = 0; i < AnyBasisSecondClass.Length; i++)
                    AnyBasisSecondClass[i] = new bool[8];

                CurrentHandshakes = new SnapHandshake[4];
                PlayerHandshakes = new ReArrayIdPool<SnapHandshake>[4];
                for (int i = 0; i < PlayerHandshakes.Length; i++)
                    PlayerHandshakes[i] = CreateSnapHandshakePool();

                for (int i = 0; i < PlayerSents.Length; i++)
                    PlayerSents[i] = BeginNewSend((byte)i);
            }
            else
            {
                ClientInputSent = Server.BeginNewSend(NetServer.SpecialNormal);
            }

            EventPacketHeader += startIndex;
            EventGhostFirstClass += startIndex;
            EventGhostSecondClass += startIndex;
            EventDeltaFirstClass += startIndex;
            EventDeltaSecondClass += startIndex;
            EventDeghostFirstClass += startIndex;
            EventDeghostSecondClass += startIndex;
            EventDestructFirstClass += startIndex;
            EventDestructSecondClass += startIndex;
            EventSnapSettings += startIndex;
            EventNewScene += startIndex;
            EventTickSettings += startIndex;
            // client events
            EventClientInput += startIndex;
            EventResendFirstClass += startIndex;
            EventResendSecondClass += startIndex;

            return 15;
        }

        public void PlayerAdded(PlayerInfo pinfo)
        {
            if (Server.IsHost)
            {
                // expand basis arrays
                if (pinfo.PlayerId >= BasisTimestampsFirstClass.Length)
                {
                    int nlen = BasisTimestampsFirstClass.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    ushort[][] nb = new ushort[nlen][];
                    for (int i = 0; i < BasisTimestampsFirstClass.Length; i++)
                        nb[i] = BasisTimestampsFirstClass[i];
                    for (int i = BasisTimestampsFirstClass.Length; i < nlen; i++)
                        nb[i] = new ushort[byte.MaxValue + 1];
                    BasisTimestampsFirstClass = nb;
                }

                if (pinfo.PlayerId >= AnyBasisFirstClass.Length)
                {
                    int nlen = AnyBasisFirstClass.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    bool[][] nb = new bool[nlen][];
                    for (int i = 0; i < AnyBasisFirstClass.Length; i++)
                        nb[i] = AnyBasisFirstClass[i];
                    for (int i = AnyBasisFirstClass.Length; i < nlen; i++)
                        nb[i] = new bool[byte.MaxValue + 1];
                    AnyBasisFirstClass = nb;
                }

                if (pinfo.PlayerId >= BasisTimestampsSecondClass.Length)
                {
                    int nlen = BasisTimestampsSecondClass.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    ushort[][] nb = new ushort[nlen][];
                    for (int i = 0; i < BasisTimestampsSecondClass.Length; i++)
                        nb[i] = BasisTimestampsSecondClass[i];
                    for (int i = BasisTimestampsSecondClass.Length; i < nlen; i++)
                        nb[i] = new ushort[BasisTimestampsSecondClass[0].Length];
                    BasisTimestampsSecondClass = nb;
                }

                if (pinfo.PlayerId >= AnyBasisSecondClass.Length)
                {
                    int nlen = AnyBasisSecondClass.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    bool[][] nb = new bool[nlen][];
                    for (int i = 0; i < AnyBasisSecondClass.Length; i++)
                        nb[i] = AnyBasisSecondClass[i];
                    for (int i = AnyBasisSecondClass.Length; i < nlen; i++)
                        nb[i] = new bool[AnyBasisSecondClass[0].Length];
                    AnyBasisSecondClass = nb;
                }

                if (CurrentHandshakes.Length <= pinfo.PlayerId)
                {
                    int nlen = CurrentHandshakes.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    SnapHandshake[] nchs = new SnapHandshake[nlen];
                    for (int i = 0; i < CurrentHandshakes.Length; i++)
                        nchs[i] = CurrentHandshakes[i];
                    CurrentHandshakes = nchs;
                }

                if (PlayerHandshakes.Length <= pinfo.PlayerId)
                {
                    int nlen = PlayerHandshakes.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    ReArrayIdPool<SnapHandshake>[] nh = new ReArrayIdPool<SnapHandshake>[nlen];
                    for (int i = 0; i < PlayerHandshakes.Length; i++)
                        nh[i] = PlayerHandshakes[i];
                    for (int i = PlayerHandshakes.Length; i < nlen; i++)
                        nh[i] = CreateSnapHandshakePool();
                    PlayerHandshakes = nh;
                }

                if (PlayerSents.Length <= pinfo.PlayerId)
                {
                    int nlen = PlayerSents.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    Sent[] ns = new Sent[nlen];
                    for (int i = 0; i < PlayerSents.Length; i++)
                        ns[i] = PlayerSents[i];
                    for (int i = PlayerSents.Length; i < nlen; i++)
                        ns[i] = BeginNewSend((byte)i);
                    PlayerSents = ns;
                }

                if (TimeSincePlayerSents.Length <= pinfo.PlayerId)
                {
                    int nlen = TimeSincePlayerSents.Length * 2;
                    while (nlen <= pinfo.PlayerId) nlen *= 2;
                    float[] nt = new float[nlen];
                    for (int i = 0; i < TimeSincePlayerSents.Length; i++)
                        nt[i] = TimeSincePlayerSents[i];
                    TimeSincePlayerSents = nt;
                }

                // expand input managers
                for (int i = 0; i < InputManagerCount; i++)
                    InputManagers[i].PlayerAdded(pinfo.PlayerId);

                ServerSendSceneSettings(pinfo.PlayerId);
                ServerSendTickSettings(pinfo.PlayerId);
            }
        }

        public void PlayerRemoved(PlayerInfo pinfo)
        {

        }

        public void ClientConnected()
        {
            // update our sents
            if (ClientInputSent != null)
                ClientInputSent.Data[0] = Server.OurPlayerId;

            if (PlayerSents != null)
            {
                for (int i = 0; i < PlayerSents.Length; i++)
                {
                    if (PlayerSents[i] != null)
                        PlayerSents[i].Data[0] = Server.OurPlayerId;
                }
            }
        }

        public void PostTick(float elapsedMS)
        {
            if (!Active)
                return;

            if (Server.IsHost)
            {
                // if we're the host, send player sents on a regular schedule
                // consider whether this is necessary
                // because the thing is: we force new sends whenever the timestamp
                // changes, so the header updates
                // and the timestamp will change every snaptick
                // so the snaptick time is about equal to the batch tick time,
                // there's no point in this, right?


                /*for (int i = 0; i < TimeSincePlayerSents.Length; i++)
                {
                    TimeSincePlayerSents[i] += elapsedMS;
                    if (TimeSincePlayerSents[i] >= TimeSincePlayerSentMax)
                    {
                        TimeSincePlayerSents[i] = 0;

                        Sent psent = PlayerSents[i];

                        // only send if it has any packets in it
                        if (psent.Length > SentSizeWithHeader)
                        {
                            Server.SendRetry(psent, Server.PlayerInfos.Values[Server.PlayerInfos.IdsToIndices[i]]);

                            // create a new send
                            PlayerSents[i] = BeginNewSend();
                        }
                    }
                }*/
            }
            else
            {
                // send client inputs if necessary
                ClientInputSentTickMS += elapsedMS;
                if (ClientInputSentTickMS >= TickMSTarget / 2f && ClientInputSent.Length > Sent.EmptySizeWithAck)
                {
                    ClientInputSentTickMS = 0;

                    Server.SendReliableAll(ClientInputSent);
                    ClientInputSent = Server.BeginNewSend(NetServer.SpecialNormal);
                }
            }
        }

        public void PreTick(float elapsedMS)
        {
            if (!Active)
                return;
            
            int times = 0;
            TickMS += elapsedMS;
            while (TickMS >= TickMSTarget)
            {
                TickMS -= TickMSTarget;

                // increment the currentTime
                if (CurrentTime == ushort.MaxValue)
                {
                    // we have a roll over
                    CurrentTime = 0;

                    // every time we roll over, increment the sequence number
                    if (SequenceNumber == byte.MaxValue)
                        SequenceNumber = 0;
                    else
                        SequenceNumber++;
                }
                else
                {
                    CurrentTime++;
                }

                // call advance on each snapper
                for (int i = 0; i < SnapperCount; i++)
                    Snappers[i].Advance(CurrentTime);

                if (Server.IsHost)
                {
                    // server logic, only do anything if we've ticked

                    // now that currentTime has changed, we need to update all of our Sents
                    // because each Sent has a header which contains the time, and those 
                    // headers would now be outdated.
                    ForceAllNewSends();

                    LoadTimestamp(CurrentTime);
                    Simulator.ServerAdvance();
                    // now we can tell our inputmanager to release all inputs
                    // from CurrentTime, since we won't need them anymore.
                    for (int o = 0; o < InputManagerCount; o++)
                        InputManagers[o].ServerReleaseInputs(CurrentTime);
                }

                times++;
            }

            if (!Server.IsHost)
            {
                // handle client logic
                // client logic occurs every frame/tick

                // start by calculating clientTime
                // instead of calculating clientTime by hand, let's 
                // measure it by subtracting the client adjustment
                // from the server time.

                // figure out how many ticks we actually step backwards
                // e.g. adj=100ms, target=30ms, timestamp=10
                //      tms=0ms -> ctime=6 ctms=20ms
                //      tms=10ms -> ctime=7 ctms=0ms
                //      tms=20ms -> ctime=7 ctms=10ms
                ushort clientStartTime = ClientTime;

                float adj = ClientTickMSAdjustment - TickMS;
                ushort ticks = (ushort)Math.Ceiling(adj / TickMSTarget);
                float over = TickMSTarget - (adj - (ticks * TickMSTarget));

                ClientTime = CurrentTime;
                if (ticks > ClientTime)
                {
                    // special case, going to roll back over
                    ticks -= ClientTime;
                    ClientTime = ushort.MaxValue;
                    ClientTime -= ticks;
                    ClientSequenceNumber = SequenceNumber;
                    // must be on previous sequence number
                    if (ClientSequenceNumber > 0)
                        ClientSequenceNumber--;
                    else
                        ClientSequenceNumber = byte.MaxValue;
                }
                else
                {
                    // simple case, don't need to roll over
                    ClientTime -= ticks;
                    ClientSequenceNumber = SequenceNumber;
                }

                ClientTickMS = over;

                float inadj = ClientTickMSAdjustment + TickMS;
                ushort inticks = (ushort)Math.Ceiling(inadj / TickMSTarget);
                float inover = TickMSTarget - (inadj - (inticks * TickMSTarget));

                ClientInputTime = CurrentTime;
                if (inticks > (ushort.MaxValue - ClientInputTime))
                {
                    // special case, going to roll forward over
                    inticks -= (ushort)((ushort.MaxValue + 1) - ClientInputTime);
                    ClientInputTime = inticks;
                    ClientInputSequenceNumber = SequenceNumber;
                    // must be on next sequence number
                    if (ClientInputSequenceNumber == byte.MaxValue)
                        ClientInputSequenceNumber = 0;
                    else
                        ClientInputSequenceNumber++;
                }
                else
                {
                    // simple case, don't need to roll over
                    ClientInputTime += inticks;
                    ClientInputSequenceNumber = SequenceNumber;
                }

                ClientInputTickMS = inover;
                ClientInputTickMSushort = (ushort)(ClientInputTickMS * 100.0f);
                ClientInputTickMSInputOffset = 0;

                // now render
                if (!ResimulateRequested)
                {
                    LoadTimestamp(ClientTime);
                    Simulator.ClientAdvance(ticks, over);
                }
                else
                {
                    // if we must resimulate, we have to scroll back further
                    // figure out how far behind ResimulateStartTimestamp is from ClientTime
                    ushort simStart = ClientTime;
                    if (simStart > ushort.MaxValue / 2)
                    {
                        if (ResimulateStartTimestamp < simStart
                            && ResimulateStartTimestamp > simStart - (ushort.MaxValue / 2))
                        {
                            simStart = ResimulateStartTimestamp;
                            ticks += (ushort)(ClientTime - ResimulateStartTimestamp);
                        }
                        // otherwise: ClientTime is older than the resim so we don't need to
                        // scroll back any further
                    }
                    else
                    {
                        if (ResimulateStartTimestamp < simStart)
                        {
                            simStart = ResimulateStartTimestamp;
                            ticks += (ushort)(ClientTime - ResimulateStartTimestamp);
                        }
                        else if (ResimulateStartTimestamp > simStart + (ushort.MaxValue / 2))
                        {
                            simStart = ResimulateStartTimestamp;
                            // calculating times is a bit trickier here due to rollover
                            ticks += (ushort)(ClientTime + (ushort.MaxValue - ResimulateStartTimestamp));
                        }
                        // otherwise: ClientTime is older
                    }

                    LoadTimestamp(simStart);
                    Simulator.ClientAdvance(ticks, over);

                    ResimulateRequested = false;
                }

                // now we can tell our inputmanager to release our inputs from
                // before ClientTime, since we won't need to resimulate that.
                ushort releaseTime = clientStartTime;
                while(releaseTime != ClientTime)
                {
                    for (int o = 0; o < InputManagerCount; o++)
                        InputManagers[o].ClientReleaseInputs(releaseTime);

                    if (releaseTime == ushort.MaxValue)
                        releaseTime = 0;
                    else
                        releaseTime++;
                }
            }

            /*todo; // VV obsolete
            if (times > 0)
            {
                // now that currentTime has changed, we need to update all of our Sents
                // because each Sent has a header which contains the time, and those 
                // headers would now be outdated.
                ForceAllNewSends();

                todo; // if we're the client, we should be loading from last confirmed input
                // receipt time rather than start time
                // and if we're the server, we should be loading from whatever our input receipt
                // cutoff time is (e.g. if we can't accept inputs older than 200ms or w/e)
                // or, it'd be more efficient on serverside if we tracked input receipts
                // and only backtracked for one round of simulation after receiving new inputs

                todo; // actually this raises some interesting questions on the server side about
                // simulation. If we need to await user inputs, where are we actually simulating?
                // if we simulate once and then simulate again when new inputs arrive, it seems
                // to imply that we'd be sending the same timestamp/snapshot twice. This would 
                // probably severely fuck with our handshaking assumptions.

                // it has to be simpler than that? or else we're kinda fucked
                // contemplate this: client is rendering in future 100ms
                // this means when they make an input at c=0ms
                // it doesn't happen until s+100ms
                // so their inputs have a 100ms time to arrive before we render
                // as long as this holds, the server never needs to back-simulate
                // instead, we just throw out inputs if they arrive too late.

                // after we receive it and network it back to the client,
                // they won't receive it until about c+160ms
                // so in general they'll be simulating about 160ms of gameplay 
                // every single frame (since last confirmed snapshot will always be 160ms behind)
                // (is this true?)

                // so, practicum: how do we apply this? 
                // the client probably needs to track "server time" and its own "client time"
                // and it can choose how far ahead its "client time" is (maybe server imposes
                // a cap at 500ms to prevent clownery)
                // its inputs will simply state what timestamp it's supposed to be and the 
                // server will store them until that time.
                // if client is especially laggy, it can extend client time

                // meanwhile the server does no time trickery. it stays at pure +/-0 time
                todo; // ^^ this sounds like a reasonable plan. let's do it

                // does this work out though
                // Client A fires at 0ms.
                // they predictively create a projectile
                // --> MASSIVE NOTE: how do we REMOVE the client's predictive
                //                   projectile once the server's ACTUAL appears?
                //                   they won't have same eid
                //     IDEA: client predictions should always disappear after
                //           100ms/clienttime gap. In good latency, they'll be 
                //           seamlessly replaced by real objects
                //     PRACTICUM: we need to create a client-prediction-entities
                //                structure and system of some kind
                todo; // ^^ SEE PRACTICUM

                // the server processes this input at 100ms.
                // it networks the projectile

                // Client B receives the snapshot at 160ms
                // they have a render delay of, say, 100ms,
                // so B renders it starting at 200ms (the snapshot was for 100ms)
                // --> How is this executed in practice?
                //     so we don't render Snapshot 0 (made at +0ms) until +100ms
                //     on the clientside. So the client is rendering server 
                //     snapshots in the past.
                //     BUT: it needs to render itself in the future...

                // it seems like what we're working towards here is a division 
                // between "server objects" and "client objects"
                // client objects are rendered +100ms from servertime
                // server objects are rendered -100ms from servertime

                // this way our inputs seem to happen immediately
                // and other player's objects have time for snapshots to arrive
                // so that they move seamlessly

                // but what do we do if you e.g. push another player character?
                // they wouldn't get shoved for 200ms in this system, because
                // they're "server objects"

                // maybe all objects are rendered as client objects:
                // 1. load all objects from snapshots of -100ms servertime
                // 2. simulate forward 100ms with all current client inputs
                // 3. objects that aren't impacted by our inputs won't move
                // 4. objects that are impacted by our inputs will move predictvely

                // on the server side, load all the most recent snapshots
                // and process the inputs from 100ms ago
                // more specifically:
                // for each player process their inputs from their [clienttime]
                // ago
                todo; // this seems like my most plausible plan yet
                
                //LoadTimestamp(startTime);
                //Simulator.Advance(times, TickMSTarget, Server.IsHost);
                //if (!Server.IsHost)
                //    Simulator.Render(TickMS, TickMSTarget);

                if (!Server.IsHost)
                {
                    LoadTimestamp(ClientTime);

                }
                else
                {
                    LoadTimestamp(CurrentTime);

                }
            }
            else if (!Server.IsHost)
            {
                todo; // instead of loading from startTime... we should probably be loading
                // from the last confirmed input receipt time to ensure proper resimulation
                LoadTimestamp(startTime);
                Simulator.Render(TickMS, TickMSTarget);
            }*/
        }

        public int Receive(Receipt receipt, PlayerInfo pinfo, ushort eventid, int c)
        {
            if (Server.IsHost)
            {
                if (eventid == EventClientInput)
                {
                    ushort timestamp = Bytes.ReadUShort(receipt.Data, c); c += 2;
                    ushort tickmsushort = Bytes.ReadUShort(receipt.Data, c); c += 2;
                    float tickms = ((float)tickmsushort) / 100.0f;
                    byte inputIndex = receipt.Data[c]; c++;

                    // find the corresponding input manager
                    if (inputIndex >= InputManagerCount)
                    {
                        // invalid input manager
                        Log("Received invalid input index '" + inputIndex + "'");
                        // must skip the packet
                        return receipt.Length + 1;
                    }
                    
                    return InputManagers[inputIndex].ReadInput(receipt, c, timestamp, tickms);
                }
                else if (eventid == EventResendFirstClass)
                {
                    byte eid = receipt.Data[c]; c++;
                    ushort timestamp = Bytes.ReadUShort(receipt.Data, c); c += 2;
                    short etype = EntityIdToSnapperIdFirstClass[eid];
                    if (etype == -1)
                    {
                        Log("Failed to resend snap entity 1:?:" + eid);
                        Log("Entity has not been ghosted yet");
                        return c;
                    }
                    ServerResendGhostFirst(receipt.PlayerId, eid, (byte)etype, timestamp);

                    return c;
                }
                else if (eventid == EventResendSecondClass)
                {
                    ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;
                    ushort timestamp = Bytes.ReadUShort(receipt.Data, c); c += 2;

                    if (EntityIdToSnapperIdSecondClass.Length <= eid)
                    {
                        Log("Failed to resend snap entity 2:?:" + eid);
                        Log("Entity has not been ghosted yet");
                        return c;
                    }

                    short etype = EntityIdToSnapperIdSecondClass[eid];
                    if (etype == -1)
                    {
                        Log("Failed to resend snap entity 2:?:" + eid);
                        Log("Entity has not been ghosted yet");
                        return c;
                    }
                    ServerResendGhostSecond(receipt.PlayerId, eid, (byte)etype, timestamp);

                    return c;
                }
                // if it's unrecognized, skip the packet
                return receipt.Length + 1;
            }

            // Packet Header
            if (eventid == EventPacketHeader)
            {
                ReceiveCurrentTime = Bytes.ReadUShort(receipt.Data, c); c += 2;
                ReceiveSequenceNumber = receipt.Data[c]; c++;
                ReceiveSceneNumber = receipt.Data[c]; c++;
                ReceiveValid = (ReceiveSceneNumber == SceneNumber);

                // check if the sequence numbers are within 1 of eachother
                // (only if already ReceiveValid, because if already false
                //  there's no need to check if it's extra-false)
                if (ReceiveValid)
                {
                    if (ReceiveSequenceNumber == 0
                        && SequenceNumber > 1 
                        && SequenceNumber != byte.MaxValue)
                    {
                        ReceiveValid = false;
                    }
                    else if (ReceiveSequenceNumber == byte.MaxValue
                        && SequenceNumber < byte.MaxValue - 1
                        && SequenceNumber != 0)
                    {
                        ReceiveValid = false;
                    }
                    else if (ReceiveSequenceNumber > 0
                        && ReceiveSequenceNumber < byte.MaxValue
                        && (ReceiveSequenceNumber < SequenceNumber - 1
                        || ReceiveSequenceNumber > SequenceNumber + 1))
                    {
                        ReceiveValid = false;
                    }
                }

                if (!ReceiveValid)
                {
                    // if this isn't a valid packet, skip the rest of it immediately
                    return receipt.Length + 1;
                }
                return c;
            }
            // Deltas
            else if (eventid == EventDeltaFirstClass)
            {
                byte eid = receipt.Data[c]; c++;
                ushort basis = Bytes.ReadUShort(receipt.Data, c); c += 2;
                byte nlen = receipt.Data[c]; c++;
                short etype = EntityIdToSnapperIdFirstClass[eid];
                if (etype == -1)
                {
                    Log("Failed to delta snap entity 1:?:" + eid + " with payload " + nlen);
                    Log("Entity has not been ghosted yet");
                    return c + nlen;
                }

                if (!Snappers[etype].UnpackDeltaFirst(eid, receipt.Data, c, nlen, ReceiveCurrentTime, basis))
                {
                    //Log("Failed to delta snap entity 1:" + etype + ":" + eid + " with payload " + nlen);
                    // request resend
                    Sent send = GetClientInputSentForOther(3);
                    send.WriteUShort(EventResendFirstClass);
                    send.WriteByte(eid);
                    send.WriteUShort(ReceiveCurrentTime);
                }
                return c + nlen;
            }
            else if (eventid == EventDeltaSecondClass)
            {
                ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;
                ushort basis = Bytes.ReadUShort(receipt.Data, c); c += 2;
                byte nlen = receipt.Data[c]; c++;
                if (EntityIdToSnapperIdSecondClass.Length <= eid)
                {
                    Log("Failed to delta snap entity 2:?:" + eid + " with payload " + nlen);
                    Log("Entity has not been ghosted yet");
                    return c + nlen;
                }

                short etype = EntityIdToSnapperIdSecondClass[eid];
                if (etype == -1)
                {
                    Log("Failed to delta snap entity 2:?:" + eid + " with payload " + nlen);
                    Log("Entity has not been ghosted yet");
                    return c + nlen;
                }

                if (!Snappers[etype].UnpackDeltaSecond(eid, receipt.Data, c, nlen, ReceiveCurrentTime, basis))
                {
                    //Log("Failed to delta snap entity 2:" + etype + ":" + eid + " with payload " + nlen);
                    // request resend
                    Sent send = GetClientInputSentForOther(4);
                    send.WriteUShort(EventResendSecondClass);
                    send.WriteUShort(eid);
                    send.WriteUShort(ReceiveCurrentTime);
                }
                return c + nlen;
            }
            // Ghost
            else if (eventid == EventGhostFirstClass)
            {
                byte eid = receipt.Data[c]; c++;
                byte etype = receipt.Data[c]; c++;
                byte nlen = receipt.Data[c]; c++;

                // store the eid:etype mapping for later usage
                EntityIdToSnapperIdFirstClass[eid] = etype;

                // todo: should we catch if this vv method returns false (failed ghost)?
                // what would we even do there? crash? log it?
                if (!Snappers[etype].UnpackGhostFirst(eid, receipt.Data, c, nlen, ReceiveCurrentTime))
                    Log("Failed to ghost snap entity 1:" + etype + ":" + eid + " with payload " + nlen);
                return c + nlen;
            }
            else if (eventid == EventGhostSecondClass)
            {
                ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;
                byte etype = receipt.Data[c]; c++;
                byte nlen = receipt.Data[c]; c++;

                // store the eid:etype mapping for later usage
                if (EntityIdToSnapperIdSecondClass.Length <= eid)
                {
                    // resize
                    int alen = EntityIdToSnapperIdSecondClass.Length * 2;
                    while (alen < eid) alen *= 2;
                    short[] na = new short[alen];
                    for (int i = 0; i < EntityIdToSnapperIdSecondClass.Length; i++)
                        na[i] = EntityIdToSnapperIdSecondClass[i];
                    for (int i = EntityIdToSnapperIdSecondClass.Length; i < na.Length; i++)
                        na[i] = -1;
                    EntityIdToSnapperIdSecondClass = na;
                }
                EntityIdToSnapperIdSecondClass[eid] = etype;

                // todo: should we catch if this vv method returns false (failed ghost)?
                // what would we even do there? crash? log it?
                if (!Snappers[etype].UnpackGhostSecond(eid, receipt.Data, c, nlen, ReceiveCurrentTime))
                    Log("Failed to ghost snap entity 2:" + etype + ":" + eid + " with payload " + nlen);
                return c + nlen;
            }
            // Resend Ghost
            else if (eventid == EventResendFirstClass)
            {
                ushort timestamp = Bytes.ReadUShort(receipt.Data, c); c += 2;
                byte eid = receipt.Data[c]; c++;
                byte etype = receipt.Data[c]; c++;
                byte nlen = receipt.Data[c]; c++;

                // store the eid:etype mapping for later usage
                EntityIdToSnapperIdFirstClass[eid] = etype;

                // todo: should we catch if this vv method returns false (failed ghost)?
                // what would we even do there? crash? log it?
                if (!Snappers[etype].UnpackGhostFirst(eid, receipt.Data, c, nlen, timestamp))
                    Log("Failed to ghost snap entity 1:" + etype + ":" + eid + " with payload " + nlen);
                return c + nlen;
            }
            else if (eventid == EventResendSecondClass)
            {
                ushort timestamp = Bytes.ReadUShort(receipt.Data, c); c += 2;
                ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;
                byte etype = receipt.Data[c]; c++;
                byte nlen = receipt.Data[c]; c++;

                // store the eid:etype mapping for later usage
                if (EntityIdToSnapperIdSecondClass.Length < eid)
                {
                    // resize
                    int alen = EntityIdToSnapperIdSecondClass.Length * 2;
                    while (alen < eid) alen *= 2;
                    short[] na = new short[alen];
                    for (int i = 0; i < EntityIdToSnapperIdSecondClass.Length; i++)
                        na[i] = EntityIdToSnapperIdSecondClass[i];
                    EntityIdToSnapperIdSecondClass = na;
                }
                EntityIdToSnapperIdSecondClass[eid] = etype;

                // todo: should we catch if this vv method returns false (failed ghost)?
                // what would we even do there? crash? log it?
                if (!Snappers[etype].UnpackGhostSecond(eid, receipt.Data, c, nlen, timestamp))
                    Log("Failed to ghost snap entity 2:" + etype + ":" + eid + " with payload " + nlen);
                return c + nlen;
            }
            // Deghost
            else if (eventid == EventDeghostFirstClass)
            {
                byte eid = receipt.Data[c]; c++;
                short etype = EntityIdToSnapperIdFirstClass[eid];
                if (etype == -1)
                {
                    Log("Failed to deghost snap entity 1:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                Snappers[etype].DeghostFirst(eid, ReceiveCurrentTime);

                return c;
            }
            else if (eventid == EventDeghostSecondClass)
            {
                ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;

                if (EntityIdToSnapperIdSecondClass.Length <= eid)
                {
                    Log("Failed to deghost snap entity 2:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                short etype = EntityIdToSnapperIdSecondClass[eid];
                if (etype == -1)
                {
                    Log("Failed to deghost snap entity 2:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                Snappers[etype].DeghostSecond(eid, ReceiveCurrentTime);

                return c;
            }
            // Destruct
            else if (eventid == EventDestructFirstClass)
            {
                byte eid = receipt.Data[c]; c++;
                short etype = EntityIdToSnapperIdFirstClass[eid];
                if (etype == -1)
                {
                    Log("Failed to destruct snap entity 1:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                Snappers[etype].DestructFirst(eid);
                EntityIdToSnapperIdFirstClass[eid] = -1;

                return c;
            }
            else if (eventid == EventDestructSecondClass)
            {
                ushort eid = Bytes.ReadUShort(receipt.Data, c); c += 2;

                if (EntityIdToSnapperIdSecondClass.Length <= eid)
                {
                    Log("Failed to destruct snap entity 2:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                short etype = EntityIdToSnapperIdSecondClass[eid];
                if (etype == -1)
                {
                    Log("Failed to destruct snap entity 2:?:" + eid);
                    Log("Entity has not been ghosted yet");
                    return c;
                }

                Snappers[etype].DestructSecond(eid);
                EntityIdToSnapperIdSecondClass[eid] = -1;

                return c;
            }
            // Scene Control
            else if (eventid == EventNewScene)
            {
                return ClientReceiveNewScene(receipt, c);
            }
            else if (eventid == EventSnapSettings)
            {
                return ClientReceiveSnapSettings(receipt, c);
            }
            // Engine Control
            else if (eventid == EventTickSettings)
            {
                return ClientReceiveTickSettings(receipt, c);
            }

            throw new Exception("NetExecutorSnapper was passed eventid '" + eventid + "' which does not belong to it.");
        }
        #endregion NetExec Interface Methods



        // Control Methods
        #region Control Methods
        public void Activate()
        {
            Active = true;
        }

        public void Deactivate()
        {
            Active = false;
        }

        public void ServerSetTickTarget(float tickms)
        {
            TickMSTarget = tickms;
            SentRetryTimer = tickms;
            if (SentRetryTimer > 40f)
                SentRetryTimer = 40f;

            ServerSendTickSettings();
        }
        #endregion Control Methods


        // Input Manager Methods
        #region Input Manager Methods
        public byte AddInputManager(ISnapInputManager input)
        {
            if (InputManagers.Length == InputManagerCount)
            {
                // must expand
                ISnapInputManager[] nsim = new ISnapInputManager[InputManagerCount * 2];
                for (int i = 0; i < InputManagers.Length; i++)
                    nsim[i] = InputManagers[i];
                InputManagers = nsim;
            }

            InputManagers[InputManagerCount] = input;
            byte index = (byte)InputManagerCount;
            input.Loaded(this, index);
            InputManagerCount++;
            return index;
        }

        public void ClearInputManagers()
        {
            InputManagerCount = 0;
        }

        // consumer note: you don't need to include the size for the event id
        // and input index byte, the method will account for it
        public Sent GetClientInputSent(byte inputIndex, int requestedLength)
        {
            // add length equal to the header size
            requestedLength += 7;

            if (requestedLength > UdpClientExtensions.MaxUdpSize)
                throw new Exception("Packet splitting not supported yet");
            Sent send = ClientInputSent;
            if (send.Length + requestedLength <= UdpClientExtensions.MaxUdpSize)
            {
                // write the header for our requester
                send.WriteUShort(EventClientInput);
                send.WriteUShort(ClientInputTime);
                send.WriteUShort(ClientInputTickMSushort);
                ClientInputTickMSInputOffset += 100.0f;
                ClientInputTickMSushort++; // important concept here:
                // each time we write out an input, increment the tickms ushort by 1
                // this ensures that our inputs are ordered properly.
                send.WriteByte(inputIndex);
                return send;
            }
            
            // if the client input buffer is already too full, send it
            Server.SendReliableAll(send);
            // create a new send
            send = Server.BeginNewSend(NetServer.SpecialNormal);
            ClientInputSent = send;

            // write the header for our requester
            send.WriteUShort(EventClientInput);
            send.WriteUShort(ClientTime);
            send.WriteUShort(ClientInputTickMSushort);
            ClientInputTickMSInputOffset += 100.0f;
            ClientInputTickMSushort++; // see above note on tickmsushort
            send.WriteByte(inputIndex);
            return send;
        }

        // it is convenient for some other client messages to hijack the command stream
        // rather than implement a second stream for the occasional e.g. resend request
        private Sent GetClientInputSentForOther(int requestedLength)
        {
            if (requestedLength > UdpClientExtensions.MaxUdpSize)
                throw new Exception("Packet splitting not supported yet");
            Sent send = ClientInputSent;
            if (send.Length + requestedLength <= UdpClientExtensions.MaxUdpSize)
            {
                return send;
            }

            // if the client input buffer is already too full, send it
            Server.SendReliableAll(send);
            // create a new send
            send = Server.BeginNewSend(NetServer.SpecialNormal);
            ClientInputSent = send;
            
            return send;
        }
        #endregion Input Manager Methods



        // Simulator Methods
        #region Simulator Methods
        public void LoadSimulator(ISnapSimulator sim)
        {
            Simulator = sim;
            sim.Loaded(this);
        }

        public void AdvanceSimulateTimestamp()
        {
            if (SimulateTimestamp == ushort.MaxValue)
                SimulateTimestamp = 0;
            else
                SimulateTimestamp++;

            for (int i = 0; i < SnapperCount; i++)
                Snappers[i].LoadTimestamp(SimulateTimestamp);
        }

        public void LoadTimestamp(ushort timestamp)
        {
            SimulateTimestamp = timestamp;

            for (int i = 0; i < SnapperCount; i++)
                Snappers[i].LoadTimestamp(timestamp);
        }

        public void RequestResimulate(ushort timestamp)
        {
            if (!ResimulateRequested)
            {
                ResimulateRequested = true;
                ResimulateStartTimestamp = timestamp;
                return;
            }

            // need to do our annoying circle-ushort comparison logic to figure out which
            // one is older
            if (ResimulateStartTimestamp > ushort.MaxValue / 2)
            {
                if (timestamp < ResimulateStartTimestamp 
                    && timestamp > ResimulateStartTimestamp - (ushort.MaxValue / 2))
                {
                    ResimulateStartTimestamp = timestamp;
                }
            }
            else
            {
                if (timestamp < ResimulateStartTimestamp 
                    || timestamp > ResimulateStartTimestamp + (ushort.MaxValue / 2))
                {
                    ResimulateStartTimestamp = timestamp;
                }
            }
        }
        #endregion Simulator Methods



        // Snapper Helpers
        #region Snapper Helpers
        private void ExpandSnappers(int targetindex)
        {
            int nlen = Snappers.Length;
            while (nlen <= targetindex) nlen *= 2;

            ISnapper[] ns = new ISnapper[nlen];
            for (int i = 0; i < Snappers.Length; i++)
                ns[i] = Snappers[i];

            Snappers = ns;
        }

        public byte AddSnapper(ISnapper sn)
        {
            byte index = (byte)SnapperCount;
            if (index >= byte.MaxValue)
                throw new Exception("Tried to add new Snapper, but already have too many!");

            SnapperCount++;
            
            if (index >= Snappers.Length)
                ExpandSnappers(index);
            
            Snappers[index] = sn;
            sn.Register(this, index);
            return index;
        }

        public void SetSnapper(ISnapper sn, ushort id)
        {
            if (id >= Snappers.Length)
                ExpandSnappers(id);

            Snappers[id] = sn;
        }

        public void RemoveSnappers()
        {
            for (int i = 0; i < Snappers.Length; i++)
            {
                if (Snappers[i] == null)
                    continue;

                Snappers[i].Removed();
                Snappers[i] = null;
            }
        }
        #endregion Snapper Helpers



        // Net Helpers
        #region Networking Helpers
        public Sent GetPlayerSent(byte pid, int requestedLength)
        {
            if (requestedLength > UdpClientExtensions.MaxUdpSize)
                throw new Exception("Packet splitting not supported yet");

            Sent send = PlayerSents[pid];
            if (send.Length + requestedLength <= UdpClientExtensions.MaxUdpSize)
                return send;
            // send the current send
            int targetIndex = Server.SendRetry(send, Server.PlayerInfos.Values[Server.PlayerInfos.IdsToIndices[pid]],
                SentMaxRetries, SentRetryTimer);
            CurrentHandshakes[pid].AckNo = send.TargetMessageId[targetIndex];

            // create a new send
            send = BeginNewSend(pid);
            PlayerSents[pid] = send;
            return send;
        }

        private void ForceAllNewSends()
        {
            for (int i = 0; i < PlayerSents.Length; i++)
            {
                ForceNewSend((byte)i);
            }
        }

        private Sent ForceNewSend(byte pid)
        {
            Sent send = PlayerSents[pid];

            if (send.Length > SentSizeWithHeader)
            {
                // send the current send, but only if anything was written other than the header
                // A note on maxRetries:
                // if this number is too small, we won't be able to trigger
                // the handshake callback, because after the retries are spent,
                // it gets removed from the processresends loop
                // and the sent is freed up, which means it is already
                // gone by the time the handshake arrives.
                
                int targetIndex = Server.SendRetry(send, Server.PlayerInfos.Values[Server.PlayerInfos.IdsToIndices[pid]],
                    SentMaxRetries, SentRetryTimer);
                if (targetIndex != -1)
                    CurrentHandshakes[pid].AckNo = send.TargetMessageId[targetIndex];

                // create a new send
                send = BeginNewSend(pid);
                PlayerSents[pid] = send;
                return send;
            }

            // otherwise, we can reuse the existing send, just change the header
            send.Length -= PacketHeaderLength;
            send.WriteUShort(EventPacketHeader);
            send.WriteUShort(CurrentTime);
            send.WriteByte(SequenceNumber);
            send.WriteByte(SceneNumber);

            // we must update the handshake memory as well
            CurrentHandshakes[pid].Timestamp = CurrentTime;

            return send;
        }

        private Sent BeginNewSend(byte pid)
        {
            Sent send = Server.BeginNewSend(NetServer.SpecialNormal);

            // add the handshake callback to the new send
            send.HandshakeCallback = HandshakeHandler;

            // when we form a new packet, append the packet header automatically
            send.WriteUShort(EventPacketHeader);
            send.WriteUShort(CurrentTime);
            send.WriteByte(SequenceNumber);
            send.WriteByte(SceneNumber);
            
            SnapHandshake h = PlayerHandshakes[pid].Request();
            h.Timestamp = CurrentTime;
            CurrentHandshakes[pid] = h;

            return send;
        }

        private void HandshakeHandler(byte pid,  ushort ackNo, bool received)
        {
            // if unreceived, return handshake to pool so it doesn't get stuck

            // 1. lookup corresponding handshake in PlayerHandshakes[pid]
            //    where h.AckNo == ackNo
            ReArrayIdPool<SnapHandshake> ph = PlayerHandshakes[pid];
            for (int i = 0; i < ph.Count; i++)
            {
                if (ph.Values[i].AckNo == ackNo)
                {
                    SnapHandshake sh = ph.Values[i];
                    // 2. for each entity, store new basis timestamps
                    //    BasisTimestampsFirstClass
                    //    AnyBasisFirstClass / BasisTimestampsSecondClass
                    //    AnyBasisSecondClass

                    if (received)
                    {
                        for (int o = 0; o < sh.FirstEntityCount; o++)
                        {
                            BasisTimestampsFirstClass[pid][sh.FirstEntities[o]] = sh.Timestamp;
                            AnyBasisFirstClass[pid][sh.FirstEntities[o]] = true;
                        }

                        for (int o = 0; o < sh.SecondEntityCount; o++)
                        {
                            BasisTimestampsSecondClass[pid][sh.SecondEntities[o]] = sh.Timestamp;
                            AnyBasisSecondClass[pid][sh.SecondEntities[o]] = true;
                        }

                        for (int o = 0; o < sh.FirstResendsCount; o++)
                        {
                            BasisTimestampsFirstClass[pid][sh.FirstResends[o]] = sh.FirstResendsTimestamp[o];
                            AnyBasisFirstClass[pid][sh.FirstResends[o]] = true;
                        }

                        for (int o = 0; o < sh.SecondResendsCount; o++)
                        {
                            BasisTimestampsSecondClass[pid][sh.SecondResends[o]] = sh.SecondResendsTimestamp[o];
                            AnyBasisSecondClass[pid][sh.SecondResends[o]] = true;
                        }
                    }

                    // 3. now release this handshake
                    ph.ReturnId(sh.GetPoolIndex());
                    break;
                }
            }
        }
        #endregion Networking Helpers



        // Server Methods
        #region Server Methods
        public void ServerBeginNewScene(byte changeType, ushort customId,
            string texta, string textb)
        {
            if (!Server.IsHost)
                throw new Exception("Tried to begin new Snapper scene as client.");

            // clear all entities
            for (int i = 0; i < Snappers.Length; i++)
                if (Snappers[i] != null)
                    Snappers[i].ClearEntities();

            // increment the scene number
            SceneNumber++;

            // clear out all of our playersends due to scene number change
            ForceAllNewSends();

            // inform our clients
            SceneChangeType = changeType;
            SceneChangeCustomId = customId;
            SceneChangeTextA = texta;
            SceneChangeTextB = textb;
            ServerSendNewScene();
        }

        private void ServerSendTickSettings(byte? pid = null)
        {
            int slen = 2 + 4;
            Sent send;
            if (pid == null)
                send = Server.GetFastOrderedAllSend(slen);
            else
                send = Server.GetFastOrderedPlayerSend(pid.Value, slen);

            send.WriteUShort(EventTickSettings);
            send.WriteFloat(TickMSTarget);
        }

        private void ServerSendNewScene(byte? pid = null)
        {
            int slen = 3 + 1 + 2;
            int talen = Bytes.GetStringLength(SceneChangeTextA);
            int tblen = Bytes.GetStringLength(SceneChangeTextB);
            slen += talen;
            slen += tblen;

            Sent send;
            if (pid == null)
                send = Server.GetFastOrderedAllSend(slen);
            else
                send = Server.GetFastOrderedPlayerSend(pid.Value, slen);

            send.WriteUShort(EventNewScene);
            send.WriteByte(SceneNumber);
            send.WriteByte(SceneChangeType);
            send.WriteUShort(SceneChangeCustomId);
            send.WriteString(SceneChangeTextA);
            send.WriteString(SceneChangeTextB);
        }

        private void ServerSendSceneSettings(byte? pid = null)
        {
            int slen = 7 + 1 + 2;
            slen += Bytes.GetStringLength(SceneChangeTextA);
            slen += Bytes.GetStringLength(SceneChangeTextB);

            Sent send;
            if (pid == null)
                send = Server.GetFastOrderedAllSend(slen);
            else
                send = Server.GetFastOrderedPlayerSend(pid.Value, slen);

            send.WriteUShort(EventSnapSettings);
            send.WriteUShort(CurrentTime);
            send.WriteByte(SequenceNumber);
            send.WriteByte(SceneNumber);
            send.WriteByte(SceneChangeType);
            send.WriteUShort(SceneChangeCustomId);
            send.WriteString(SceneChangeTextA);
            send.WriteString(SceneChangeTextB);
        }

        public void ServerSendGhostFirst(byte pid, byte eid, byte etype)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostFirst(eid, CurrentTime);
            int slen = 2 + 1 + 1 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventGhostFirstClass);
            send.WriteByte(eid);
            send.WriteByte(etype);
            send.WriteByte(nlen);

            snapper.WriteGhostFirst(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddFirstEntity(eid);
        }

        public void ServerSendGhostSecond(byte pid, ushort eid, byte etype)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostSecond(eid, CurrentTime);
            int slen = 2 + 2 + 1 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventGhostSecondClass);
            send.WriteUShort(eid);
            send.WriteByte(etype);
            send.WriteByte(nlen);

            //todo; // maybe we could move this into the snapper itself
            // would that accomplish anything?
            // well we'd only have one interface call instead of two

            snapper.WriteGhostSecond(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddSecondEntity(eid);
        }

        public void ServerSendGhostAllFirst(byte eid, byte etype)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostFirst(eid, CurrentTime);
            int slen = 2 + 1 + 1 + 1 + nlen;

            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to host

                Sent send = GetPlayerSent(pid, slen);

                send.WriteUShort(EventGhostFirstClass);
                send.WriteByte(eid);
                send.WriteByte(etype);
                send.WriteByte(nlen);

                snapper.WriteGhostFirst(send);

                // when we send the ghost, we need to add it to our handshake memory
                CurrentHandshakes[pid].AddFirstEntity(eid);
            }
        }

        public void ServerSendGhostAllSecond(ushort eid, byte etype)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostSecond(eid, CurrentTime);
            int slen = 2 + 2 + 1 + 1 + nlen;

            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to host

                Sent send = GetPlayerSent(pid, slen);

                send.WriteUShort(EventGhostSecondClass);
                send.WriteUShort(eid);
                send.WriteByte(etype);
                send.WriteByte(nlen);

                snapper.WriteGhostSecond(send);

                // when we send the ghost, we need to add it to our handshake memory
                CurrentHandshakes[pid].AddSecondEntity(eid);
            }
        }

        private void ServerResendGhostFirst(byte pid, byte eid, byte etype, ushort timestamp)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostFirst(eid, CurrentTime);
            int slen = 2 + 2 + 1 + 1 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventResendFirstClass);
            send.WriteUShort(timestamp);
            send.WriteByte(eid);
            send.WriteByte(etype);
            send.WriteByte(nlen);

            snapper.WriteGhostFirst(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddFirstResend(eid, timestamp);
        }

        private void ServerResendGhostSecond(byte pid, ushort eid, byte etype, ushort timestamp)
        {
            ISnapper snapper = Snappers[etype];
            byte nlen = snapper.PrepGhostSecond(eid, CurrentTime);
            int slen = 2 + 2 + 2 + 1 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventResendSecondClass);
            send.WriteUShort(timestamp);
            send.WriteUShort(eid);
            send.WriteByte(etype);
            send.WriteByte(nlen);

            snapper.WriteGhostSecond(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddSecondResend(eid, timestamp);
        }

        public void ServerSendDeltaFirst(byte pid, byte eid, byte etype)
        {
            if (!AnyBasisFirstClass[pid][eid])
            {
                // must send a full ghost instead, we don't have a basis timestamp
                ServerSendGhostFirst(pid, eid, etype);
                return;
            }

            ushort basisT = BasisTimestampsFirstClass[pid][eid];

            ISnapper snapper = Snappers[etype];
            if (!snapper.PrepDeltaFirst(eid, CurrentTime, basisT, out byte nlen))
            {
                // if we failed to prep the delta, must send a full
                ServerSendGhostFirst(pid, eid, etype);
                return;
            }
            int slen = 2 + 1 + 2 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventDeltaFirstClass);
            send.WriteByte(eid);
            send.WriteUShort(basisT);
            send.WriteByte(nlen);

            snapper.WriteDeltaFirst(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddFirstEntity(eid);
        }

        public void ServerSendDeltaSecond(byte pid, ushort eid, byte etype)
        {
            if (AnyBasisSecondClass.Length <= eid || !AnyBasisSecondClass[pid][eid])
            {
                // must send a full ghost instead, we don't have a basis timestamp
                ServerSendGhostSecond(pid, eid, etype);
                return;
            }

            ushort basisT = BasisTimestampsSecondClass[pid][eid];

            ISnapper snapper = Snappers[etype];
            if (!snapper.PrepDeltaSecond(eid, CurrentTime, basisT, out byte nlen))
            {
                // if we failed to prep the delta, must send a full
                ServerSendGhostSecond(pid, eid, etype);
                return;
            }
            int slen = 2 + 2 + 2 + 1 + nlen;
            Sent send = GetPlayerSent(pid, slen);

            send.WriteUShort(EventDeltaSecondClass);
            send.WriteUShort(eid);
            send.WriteUShort(basisT);
            send.WriteByte(nlen);

            snapper.WriteDeltaSecond(send);

            // when we send the ghost, we need to add it to our handshake memory
            CurrentHandshakes[pid].AddSecondEntity(eid);
        }

        public void ServerSendDeltaAllFirst(byte eid, byte etype)
        {
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                ServerSendDeltaFirst(pid, eid, etype);
            }
        }

        public void ServerSendDeltaAllSecond(ushort eid, byte etype)
        {
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                ServerSendDeltaSecond(pid, eid, etype);
            }
        }

        public void ServerSendDeghostFirst(byte pid, byte eid)
        {
            Sent send = GetPlayerSent(pid, 2 + 1);

            send.WriteUShort(EventDeghostFirstClass);
            send.WriteByte(eid);
        }

        public void ServerSendDeghostSecond(byte pid, ushort eid)
        {
            Sent send = GetPlayerSent(pid, 2 + 2);

            send.WriteUShort(EventDeghostSecondClass);
            send.WriteUShort(eid);
        }

        // as a convenience, for deghosting an entity for all players
        // usually is a prelude to destruction
        public void ServerSendDeghostFirstAll(byte eid)
        {
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                Sent send = GetPlayerSent(pid, 3);
                send.WriteUShort(EventDeghostFirstClass);
                send.WriteByte(eid);
            }
        }

        public void ServerSendDeghostSecondAll(ushort eid)
        {
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                Sent send = GetPlayerSent(pid, 4);
                send.WriteUShort(EventDeghostSecondClass);
                send.WriteUShort(eid);
            }
        }

        // returns true if an entity can be made
        public bool ServerRequestEntityFirst(byte etype, out byte eid)
        {
            eid = NextEntityIdFirstClass;
            if (eid == byte.MaxValue && EntityIdToSnapperIdFirstClass[eid] != -1)
                return false;
            EntityIdToSnapperIdFirstClass[eid] = etype;
            
            // figure out the next entity id
            if (eid != byte.MaxValue)
            {
                NextEntityIdFirstClass++;
                while (eid != byte.MaxValue && EntityIdToSnapperIdFirstClass[NextEntityIdFirstClass] != -1)
                    NextEntityIdFirstClass++;
            }
            return true;
        }

        public bool ServerRequestEntitySecond(byte etype, out ushort eid)
        {
            eid = NextEntityIdSecondClass;
            ResizeSecondClass(eid);

            if (eid == ushort.MaxValue && EntityIdToSnapperIdSecondClass[eid] != -1)
                return false;
            EntityIdToSnapperIdSecondClass[eid] = etype;

            // figure out the next entity id
            if (eid != ushort.MaxValue)
            {
                NextEntityIdSecondClass++;
                // expand entity array if needed
                ResizeSecondClass(NextEntityIdSecondClass);

                while (eid != ushort.MaxValue && EntityIdToSnapperIdSecondClass[NextEntityIdSecondClass] != -1)
                {
                    NextEntityIdSecondClass++;

                    // expand entity array if needed
                    ResizeSecondClass(NextEntityIdSecondClass);
                }
            }
            return true;
        }

        public void ServerDestructFirst(byte eid)
        {
            short etype = EntityIdToSnapperIdFirstClass[eid];
            if (etype == -1)
            {
                Log("Failed to destruct snap entity 1:?:" + eid);
                Log("Entity has not been ghosted yet");
                return;
            }
            Snappers[etype].DestructFirst(eid);

            // network
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                Sent send = GetPlayerSent(pid, 3);
                send.WriteUShort(EventDestructFirstClass);
                send.WriteByte(eid);
            }

            // update the entity mapping + next entity
            EntityIdToSnapperIdFirstClass[eid] = -1;
            if (NextEntityIdFirstClass > eid)
                NextEntityIdFirstClass = eid;
        }

        public void ServerDestructSecond(ushort eid)
        {
            short etype = EntityIdToSnapperIdSecondClass[eid];
            if (etype == -1)
            {
                Log("Failed to destruct snap entity 2:?:" + eid);
                Log("Entity has not been ghosted yet");
                return;
            }
            Snappers[etype].DestructSecond(eid);

            // network
            for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // don't send to ourself

                Sent send = GetPlayerSent(pid, 4);
                send.WriteUShort(EventDestructSecondClass);
                send.WriteUShort(eid);
            }

            // update the entity mapping + next entity
            EntityIdToSnapperIdSecondClass[eid] = -1;
            if (NextEntityIdSecondClass > eid)
                NextEntityIdSecondClass = eid;
        }
        #endregion Server Methods



        // Client Methods
        #region Client Methods
        private int ClientReceiveNewScene(Receipt receipt, int c)
        {
            SceneNumber = receipt.Data[c]; c++;
            SceneChangeType = receipt.Data[c]; c++;
            SceneChangeCustomId = Bytes.ReadUShort(receipt.Data, c); c += 2;
            SceneChangeTextA = Bytes.ReadString(receipt.Data, c, out int tlen); c += tlen;
            SceneChangeTextB = Bytes.ReadString(receipt.Data, c, out tlen); c += tlen;

            // throw out ALL entities
            for (int i = 0; i < SnapperCount; i++)
                Snappers[i].ClearEntities();

            // inform our scene changer, if we have one
            if (CallbackClientNewScene != null)
                CallbackClientNewScene(SceneChangeType, SceneChangeCustomId,
                    SceneChangeTextA, SceneChangeTextB);

            return c;
        }

        private int ClientReceiveSnapSettings(Receipt receipt, int c)
        {
            CurrentTime = Bytes.ReadUShort(receipt.Data, c); c += 2;
            SequenceNumber = receipt.Data[c]; c++;
            SceneNumber = receipt.Data[c]; c++;
            SceneChangeType = receipt.Data[c]; c++;
            SceneChangeCustomId = Bytes.ReadUShort(receipt.Data, c); c += 2;
            SceneChangeTextA = Bytes.ReadString(receipt.Data, c, out int tlen); c += tlen;
            SceneChangeTextB = Bytes.ReadString(receipt.Data, c, out tlen); c += tlen;

            // inform our scene changer, if we have one
            if (CallbackClientNewScene != null)
                CallbackClientNewScene(SceneChangeType, SceneChangeCustomId,
                    SceneChangeTextA, SceneChangeTextB);

            return c;
        }

        private int ClientReceiveTickSettings(Receipt receipt, int c)
        {
            TickMSTarget = Bytes.ReadFloat(receipt.Data, c); c += 4;
            return c;
        }
        #endregion Client Methods


        
        // Utility Methods
        #region Utility Methods
        private void Log(string s)
        {
            if (Server.NetLogger.On)
                Server.NetLogger.Log(s);
        }

        private ReArrayIdPool<SnapHandshake> CreateSnapHandshakePool()
        {
            return new ReArrayIdPool<SnapHandshake>(4, 1000,
                () => { return new SnapHandshake(); },
                (s) => { s.Clear(); });
        }

        private void ResizeSecondClass(int target)
        {
            if (target >= EntityIdToSnapperIdSecondClass.Length)
            {
                // expand
                int nlen = EntityIdToSnapperIdSecondClass.Length * 2;
                while (nlen <= target) nlen *= 2;
                short[] na = new short[nlen];
                for (int i = 0; i < EntityIdToSnapperIdSecondClass.Length; i++)
                    na[i] = EntityIdToSnapperIdSecondClass[i];
                for (int i = EntityIdToSnapperIdSecondClass.Length; i < na.Length; i++)
                    na[i] = -1;
                EntityIdToSnapperIdSecondClass = na;
            }

            for (int p = 0; p < BasisTimestampsSecondClass.Length; p++)
            {
                if (target >= BasisTimestampsSecondClass[p].Length)
                {
                    // expand
                    int nlen = BasisTimestampsSecondClass[p].Length * 2;
                    while (nlen <= target) nlen *= 2;
                    ushort[] na = new ushort[nlen];
                    for (int i = 0; i < BasisTimestampsSecondClass[p].Length; i++)
                        na[i] = BasisTimestampsSecondClass[p][i];
                    BasisTimestampsSecondClass[p] = na;
                }
            }

            for (int p = 0; p < AnyBasisSecondClass.Length; p++)
            {
                if (target >= AnyBasisSecondClass[p].Length)
                {
                    // expand
                    int nlen = AnyBasisSecondClass[p].Length * 2;
                    while (nlen <= target) nlen *= 2;
                    bool[] na = new bool[nlen];
                    for (int i = 0; i < AnyBasisSecondClass[p].Length; i++)
                        na[i] = AnyBasisSecondClass[p][i];
                    AnyBasisSecondClass[p] = na;
                }
            }
        }
        #endregion Utility Methods
    }
}
