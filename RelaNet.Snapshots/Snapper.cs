using RelaNet.Messages;
using RelaStructures;
using System;
using System.Collections.Generic;
using System.Text;

namespace RelaNet.Snapshots
{
    public class Snapper<TSnap, TStatic, TPacker, TPackInfo> : ISnapper 
        where TSnap : struct                                           // entity struct
        where TStatic : struct                                         // static data struct
        where TPacker : struct, ISnapPacker<TSnap, TStatic, TPackInfo> // packer struct 
        where TPackInfo : struct                                       // packer info struct
    {
        // Delegates
        /*public delegate void ClearDelegate(ref T obj);
        public delegate void UnpackFullDelegate(ref T obj, byte[] blob, int start, int count);
        public delegate void UnpackDeltaDelegate(ref T obj, T basis, byte[] blob, int start, int count);
        public delegate void ExtrapolateDelegate(ref T target, T basis, float elapsedms);

        public ClearDelegate ClearAction;
        public UnpackFullDelegate UnpackFullAction;
        public UnpackDeltaDelegate UnpackDeltaAction;
        public ExtrapolateDelegate ExtrapolateAction;*/

        private readonly TPacker Packer;
        private TPackInfo PackInfo;

        // defn. Entity Id
        // assigned by NetExecutorSnapper and networked
        // defn. Inner Id
        // assigned by Snapper and not networked (used in StructReArrays)

        // Entity data
        public ReArrayIdPool<SnapHistory<TSnap, TStatic>> FirstData;
        public ReArrayIdPool<SnapHistory<TSnap, TStatic>> SecondData;

        // Entity map
        public int[] FirstEntityIdToInnerId;
        public int[] SecondEntityIdToInnerId;

        public ushort CurrentTime = 0;

        // Registration
        public NetExecutorSnapper NetSnapper { get; private set; }
        public byte EntityType { get; private set; } = 0;

        // Callbacks
        //            eid   h                            timestamp
        public Action<byte, SnapHistory<TSnap, TStatic>, ushort> CallbackFirstGhosted;
        public Action<ushort, SnapHistory<TSnap, TStatic>, ushort> CallbackSecondGhosted;
        //            eid   h
        public Action<byte, SnapHistory<TSnap, TStatic>> CallbackFirstDestroyed;
        public Action<ushort, SnapHistory<TSnap, TStatic>> CallbackSecondDestroyed;

        private SnapHistory<TSnap, TStatic> TempWriteHistory;
        private int TempWriteIndex;

        public Snapper(int firstWindowLength = 64, int secondWindowLength = 32)
        {
            Packer = new TPacker();

            // setup entity data
            FirstData = new ReArrayIdPool<SnapHistory<TSnap, TStatic>>(4, byte.MaxValue + 1,
                () =>
                {
                    return new SnapHistory<TSnap, TStatic>(firstWindowLength, true);
                },
                (s) =>
                {
                    for (int i = 0; i < s.Shots.Length; i++)
                        Packer.Clear(ref s.Shots[i]);
                    s.Clear();
                });

            SecondData = new ReArrayIdPool<SnapHistory<TSnap, TStatic>>(4, ushort.MaxValue + 1,
                () =>
                {
                    return new SnapHistory<TSnap, TStatic>(secondWindowLength, false);
                },
                (s) =>
                {
                    for (int i = 0; i < s.Shots.Length; i++)
                        Packer.Clear(ref s.Shots[i]);
                    s.Clear();
                });

            // setup entityid to innerid maps
            FirstEntityIdToInnerId = new int[8];
            for (int i = 0; i < FirstEntityIdToInnerId.Length; i++)
                FirstEntityIdToInnerId[i] = -1;

            SecondEntityIdToInnerId = new int[8];
            for (int i = 0; i < SecondEntityIdToInnerId.Length; i++)
                SecondEntityIdToInnerId[i] = -1;
        }



        // Register Commands
        public void Register(NetExecutorSnapper netSnapper, byte etype)
        {
            NetSnapper = netSnapper;
            EntityType = etype;
        }

        public byte GetEntityType()
        {
            return EntityType;
        }



        // Array helpers
        private void ExpandFirstEntityIdMap(byte target)
        {
            int newlen = FirstEntityIdToInnerId.Length * 2;
            while (newlen <= target)
                newlen *= 2;

            int[] na = new int[newlen];
            for (int i = 0; i < FirstEntityIdToInnerId.Length; i++)
                na[i] = FirstEntityIdToInnerId[i];
            for (int i = FirstEntityIdToInnerId.Length; i < na.Length; i++)
                na[i] = -1;

            FirstEntityIdToInnerId = na;
        }

        private void ExpandSecondEntityIdMap(ushort target)
        {
            int newlen = SecondEntityIdToInnerId.Length * 2;
            while (newlen <= target)
                newlen *= 2;

            int[] na = new int[newlen];
            for (int i = 0; i < SecondEntityIdToInnerId.Length; i++)
                na[i] = SecondEntityIdToInnerId[i];
            for (int i = SecondEntityIdToInnerId.Length; i < na.Length; i++)
                na[i] = -1;

            SecondEntityIdToInnerId = na;
        }



        // Pack Ghosting
        public byte PrepGhostFirst(byte entityid, ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return 0;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return 0;

            SnapHistory<TSnap, TStatic> h = FirstData.Values[FirstData.IdsToIndices[innerid]];

            TempWriteHistory = h;
            TempWriteIndex = h.FindIndex(timestamp);

            return Packer.PrepPackFull(
                h.Shots[TempWriteIndex], h.StaticData, out PackInfo);
        }

        public byte PrepGhostSecond(ushort entityid, ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return 0;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return 0;

            SnapHistory<TSnap, TStatic> h = SecondData.Values[SecondData.IdsToIndices[innerid]];

            TempWriteHistory = h;
            TempWriteIndex = h.FindIndex(timestamp);

            return Packer.PrepPackFull(
                h.Shots[TempWriteIndex], h.StaticData, out PackInfo);
        }

        public void WriteGhostFirst(Sent sent)
        {
            Packer.PackFull(sent,
                TempWriteHistory.Shots[TempWriteIndex],
                TempWriteHistory.StaticData,
                PackInfo);
        }

        public void WriteGhostSecond(Sent sent)
        {
            Packer.PackFull(sent,
                TempWriteHistory.Shots[TempWriteIndex],
                TempWriteHistory.StaticData,
                PackInfo);
        }



        // Pack Delta

        public bool PrepDeltaFirst(byte entityid, ushort timestamp, ushort basisTimestamp,
            out byte len)
        {
            len = 0;

            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            SnapHistory<TSnap, TStatic> h = FirstData.Values[FirstData.IdsToIndices[innerid]];

            int index = h.FindIndex(timestamp);
            if (index == -1)
                return false;

            int basisIndex = h.FindIndex(basisTimestamp);
            if (basisIndex == -1)
                return false;

            TempWriteHistory = h;
            TempWriteIndex = index;

            len = Packer.PrepPackDelta(
                h.Shots[index],
                h.Shots[basisIndex],
                out PackInfo);

            return true;
        }

        public bool PrepDeltaSecond(ushort entityid, ushort timestamp, ushort basisTimestamp,
            out byte len)
        {
            len = 0;

            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            SnapHistory<TSnap, TStatic> h = SecondData.Values[SecondData.IdsToIndices[innerid]];

            int index = h.FindIndex(timestamp);
            if (index == -1)
                return false;

            int basisIndex = h.FindIndex(basisTimestamp);
            if (basisIndex == -1)
                return false;

            TempWriteHistory = h;
            TempWriteIndex = index;

            len = Packer.PrepPackDelta(
                h.Shots[index],
                h.Shots[basisIndex],
                out PackInfo);

            return true;
        }

        public void WriteDeltaFirst(Sent sent)
        {
            Packer.PackDelta(sent,
                TempWriteHistory.Shots[TempWriteIndex],
                PackInfo);
        }

        public void WriteDeltaSecond(Sent sent)
        {
            Packer.PackDelta(sent,
                TempWriteHistory.Shots[TempWriteIndex],
                PackInfo);
        }



        // Unpack Ghosting
        public bool UnpackGhostFirst(byte entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            if (entityid >= FirstEntityIdToInnerId.Length)
                ExpandFirstEntityIdMap(entityid);

            // if entity already exists, do unpackfull instead
            if (FirstEntityIdToInnerId[entityid] != -1)
            {
                return UnpackFull(FirstData, FirstEntityIdToInnerId[entityid],
                    blob, blobstart, blobcount, timestamp);
            }

            if (FirstData.Count > byte.MaxValue)
                return false;


            int innerid = 
                (byte)Ghost(FirstData,
                    entityid,
                    blob, blobstart, blobcount,
                    timestamp);

            FirstEntityIdToInnerId[entityid] = innerid;

            if (CallbackFirstGhosted != null)
                CallbackFirstGhosted(entityid, FirstData.Values[FirstData.IdsToIndices[innerid]], timestamp);

            return true;
        }

        public bool UnpackGhostSecond(ushort entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            if (entityid >= SecondEntityIdToInnerId.Length)
                ExpandSecondEntityIdMap(entityid);

            // if entity already exists, do unpackfull instead
            if (SecondEntityIdToInnerId[entityid] != -1)
            {
                return UnpackFull(SecondData, SecondEntityIdToInnerId[entityid],
                    blob, blobstart, blobcount, timestamp);
            }

            if (SecondData.Count > ushort.MaxValue)
                return false;
            
            int innerid = 
                (ushort)Ghost(SecondData,
                    entityid,
                    blob, blobstart, blobcount,
                    timestamp);

            SecondEntityIdToInnerId[entityid] = innerid;

            if (CallbackSecondGhosted != null)
                CallbackSecondGhosted(entityid, SecondData.Values[SecondData.IdsToIndices[innerid]], timestamp);

            return true;
        }

        private int Ghost(ReArrayIdPool<SnapHistory<TSnap, TStatic>> data,
            ushort entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            SnapHistory<TSnap, TStatic> sh = data.Request();

            sh.EntityId = entityid;
            Packer.UnpackFull(ref sh.Shots[0], ref sh.StaticData, blob, blobstart, blobcount);
            sh.Timestamps[0] = timestamp;
            sh.Flags[0] = SnapHistory<TSnap, TStatic>.FlagGold;
            sh.LeadingIndex = 0;

            // now, figure how close we are to current time
            // under normal conditions, we should be receiving snapshots
            // very close to the CurrentTime
            int expIndex = sh.FindIndex(CurrentTime);

            // however, if the expIndex is -1, it means the snapshot
            // we just received is outside the current window
            // (because CurrentTime relative to it is outside the window)
            if (expIndex == -1)
            {
                // in this case, we need to just abandon the snapshot we
                // received. This can obviously create issues since we'll
                // need a full snapshot again.
                sh.Timestamps[0] = CurrentTime;
                sh.Flags[0] = SnapHistory<TSnap, TStatic>.FlagEmpty;
            }
            else
            {
                // in this case, we're within the window, so let's
                // just set the leading index properly.
                sh.LeadingIndex = expIndex;
                sh.Timestamps[sh.LeadingIndex] = CurrentTime;
            }

            // setup the rest of the future timestamps
            ushort itime = sh.Timestamps[0];
            for (int i = 1; i <= sh.Shots.Length / 2; i++)
            {
                if (itime == ushort.MaxValue)
                    itime = 0;
                else
                    itime++;

                sh.Timestamps[i] = itime;
            }

            // and the past
            itime = sh.Timestamps[0];
            for (int i = 0; i < sh.Shots.Length / 2; i++)
            {
                if (itime == 0)
                    itime = ushort.MaxValue;
                else
                    itime--;

                sh.Timestamps[sh.Shots.Length - 1 - i] = itime;
            }

            // return the innerid
            return sh.PoolId;
        }



        // Unpack Full
        public bool UnpackFullFirst(byte entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            UnpackFull(FirstData, innerid, blob, blobstart, blobcount, timestamp);
            return true;
        }

        public bool UnpackFullSecond(ushort entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            return UnpackFull(SecondData, innerid, blob, blobstart, blobcount, timestamp);
        }

        private bool UnpackFull(ReArrayIdPool<SnapHistory<TSnap, TStatic>> data,
            int innerid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp)
        {
            SnapHistory<TSnap, TStatic> sh = data.Values[data.IdsToIndices[innerid]];
            
            int index = sh.FindIndex(timestamp);
            if (index < 0)
                return false; // out of bounds-- too far in future or past

            Packer.UnpackFull(ref sh.Shots[index], ref sh.StaticData, blob, blobstart, blobcount);
            sh.Timestamps[index] = timestamp;
            sh.Flags[index] = SnapHistory<TSnap, TStatic>.FlagGold;

            // handle flag rollover
            Rollover(sh, index, timestamp);

            return true;
        }

        private void Rollover(SnapHistory<TSnap, TStatic> sh, int index, ushort timestamp)
        {
            // this whole idea has to be reworked
            // I guess an issue is that one of our key assumptions
            // is that the indices will always be populated with timestamps
            // so we need to have some kind of rollover
            // but doing the extrap action here is not good
            // 1. it's not real simulated
            // 2. why not do interp when available?

            // one option would be to do simple extrap/interp here
            // or even just copy the same snapshot over and over
            // and then, when we get an older timestamp, flag the simulator
            // as "needs rollback calculation" (ONLY IF WE ACTUALLY HAVE ANY
            // SILVER SNAPSHOTS AHEAD OF THIS TIMESTAMP)

            // then, at the end of each processing, if the simulator has
            // this flag set, it will run again from whatever the oldest time
            // flagged was (need to track the timestamp of each flagging as well)
            // this will generate new silver snapshots, and they'll be accurate to boot.

            // RE: THIS DISCUSSION
            // we decided to do the "resimulate flagging" approach
            // so the way this works now is that if we receive a timestamp,
            // and there's unknowns in front of it,
            // the system gets asked to resimulate from that timestamp

            int nindex = index + 1;
            ushort nextTime = timestamp;
            if (nindex == sh.Shots.Length)
                nindex = 0;

            if (nextTime == ushort.MaxValue)
                nextTime = 0;
            else
                nextTime++;

            // we can only rollover onto silver flags
            if (sh.Flags[nindex] != SnapHistory<TSnap, TStatic>.FlagSilver && sh.Flags[nindex] != SnapHistory<TSnap, TStatic>.FlagEmpty)
                return;

            // can only rollover onto subsequent timestamps
            if (sh.Timestamps[nindex] != nextTime)
                return;

            // if we get here, then we hit a silver or an empty that must be filled out
            // tell the snap system to resimulate 
            NetSnapper.RequestResimulate(timestamp);
                

            // VVVV OLD VVVV
            /*
            // flags need to act a certain way to allow us to have
            // snapshots roll over when no changes are needed
            // e.g.:
            // 1 = from server (GOLD STANDARD)
            // 2 = roll over, this occurs when no snapshot is received
            //     we assume shot is unchanged and use it again for next
            //     timestamp
            // now imagine we have some snaps:
            // aaaaabb
            // 1222212
            //   ^ and here we receive a new snapshot c
            // we SCROLL FORWARD and replace all 2s with the new snapshot:
            // aacccbb
            // 1212212
            // this way we can let client continue to scroll forward but 
            // also correct itself properly
            int nindex = index + 1;
            ushort nextTime = timestamp;
            int rolls = 1;
            while (true)
            {
                if (nindex == sh.Shots.Length)
                    nindex = 0;

                if (nextTime == ushort.MaxValue)
                    nextTime = 0;
                else
                    nextTime++;

                // we can only rollover onto silver flags
                if (sh.Flags[nindex] != SnapHistory<T>.FlagSilver && sh.Flags[nindex] != SnapHistory<T>.FlagEmpty)
                    break;

                // can only rollover onto subsequent timestamps
                if (sh.Timestamps[nindex] != nextTime)
                    break;

                // now we're ready to roll
                ExtrapolateAction(ref sh.Shots[nindex], sh.Shots[index], TickMS * rolls);
                rolls++;

                nindex++;
            }*/
        }



        // Unpack Delta
        public bool UnpackDeltaFirst(byte entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp, ushort basisTimestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            return UnpackDelta(FirstData, innerid, blob, blobstart, blobcount, timestamp, basisTimestamp);
        }

        public bool UnpackDeltaSecond(ushort entityid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp, ushort basisTimestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            return UnpackDelta(SecondData, innerid, blob, blobstart, blobcount, timestamp, basisTimestamp);
        }

        private bool UnpackDelta(ReArrayIdPool<SnapHistory<TSnap, TStatic>> data,
            int innerid,
            byte[] blob, int blobstart, int blobcount,
            ushort timestamp, ushort basisTimestamp)
        {
            SnapHistory<TSnap, TStatic> sh = data.Values[data.IdsToIndices[innerid]];

            int index = sh.FindIndex(timestamp);
            if (index < 0 || index >= sh.Shots.Length)
                return false; // out of bounds

            int basisIndex = sh.FindIndex(basisTimestamp);
            if (basisIndex < 0 || basisIndex >= sh.Shots.Length)
                return false; // out of bounds

            Packer.UnpackDelta(ref sh.Shots[index], sh.Shots[basisIndex], blob, blobstart, blobcount);
            sh.Timestamps[index] = timestamp;
            sh.Flags[index] = SnapHistory<TSnap, TStatic>.FlagGold;

            // handle flag rollover
            Rollover(sh, index, timestamp);

            return true;
        }



        // Deghost
        public bool DeghostFirst(byte entityid, ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            return Deghost(FirstData, innerid, timestamp);
        }

        public bool DeghostSecond(ushort entityid, ushort timestamp)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            return Deghost(SecondData, innerid, timestamp);
        }

        private bool Deghost(ReArrayIdPool<SnapHistory<TSnap, TStatic>> data,
            int innerid, ushort timestamp)
        {
            SnapHistory<TSnap, TStatic> sh = data.Values[data.IdsToIndices[innerid]];

            int index = sh.FindIndex(timestamp);
            if (index < 0 || index >= sh.Shots.Length)
                return false; // out of bounds

            sh.Flags[index] = SnapHistory<TSnap, TStatic>.FlagDeghosted;

            // scroll forward and deghost subsequent timestamps!

            // what to do if deghost and ghost arrive out of order?
            // simple: by only overwriting SILVER flags, we ensure that deghosting
            // will not overwrite fresh / more recent data

            int nindex = index + 1;
            ushort nextTime = timestamp;
            while (true)
            {
                if (nindex == sh.Shots.Length)
                    nindex = 0;

                if (nextTime == ushort.MaxValue)
                    nextTime = 0;
                else
                    nextTime++;

                // we can only rollover onto silver flags
                if (sh.Flags[nindex] != SnapHistory<TSnap, TStatic>.FlagSilver && sh.Flags[nindex] != SnapHistory<TSnap, TStatic>.FlagEmpty)
                    break;

                // can only rollover onto subsequent timestamps
                if (sh.Timestamps[nindex] != nextTime)
                    break;

                // now we're ready to roll
                sh.Flags[nindex] = SnapHistory<TSnap, TStatic>.FlagDeghosted;

                nindex++;
            }

            return true;
        }   



        // Destruct
        public bool DestructFirst(byte entityid)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= FirstEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = FirstEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            if (CallbackFirstDestroyed != null)
                CallbackFirstDestroyed(entityid, FirstData.Values[FirstData.IdsToIndices[innerid]]);

            FirstData.ReturnId(innerid);
            FirstEntityIdToInnerId[entityid] = -1;
            return true;
        }

        public bool DestructSecond(ushort entityid)
        {
            // if ``, can't possibly belong to this snapper
            if (entityid >= SecondEntityIdToInnerId.Length)
                return false;

            // if ``, this snapper doesn't have this entity
            int innerid = SecondEntityIdToInnerId[entityid];
            if (innerid == -1)
                return false;

            if (CallbackSecondDestroyed != null)
                CallbackSecondDestroyed(entityid, SecondData.Values[SecondData.IdsToIndices[innerid]]);

            SecondData.ReturnId(innerid);
            SecondEntityIdToInnerId[entityid] = -1;
            return true;
        }



        // Prime Logic

        // Advancement occurs every tick
        public void Advance(ushort currentTime)
        {
            CurrentTime = currentTime;
            
            // when we advance, push every leading edge forward
            for (int i = 0; i < FirstData.Count; i++)
            {
                SnapHistory<TSnap, TStatic> sh = FirstData.Values[i];
                ushort last = sh.Timestamps[sh.LeadingIndex];
                ushort next = last == ushort.MaxValue ? (ushort)0 : (ushort)(last + 1);
                int startIndex = sh.LeadingIndex;

                sh.LeadingIndex++;
                if (sh.LeadingIndex == sh.Shots.Length)
                    sh.LeadingIndex = 0;

                if (sh.Timestamps[sh.LeadingIndex] != next)
                {
                    // we haven't received the next snap yet, so destroy it and 
                    // ensure that it is empty for the simulator
                    sh.Timestamps[sh.LeadingIndex] = next;
                    sh.Flags[sh.LeadingIndex] = SnapHistory<TSnap, TStatic>.FlagEmpty;

                    // note: we don't need to actually call Clear on the snapshot
                    // since the fact that it's marked with an empty flag means
                    // the simulator should avoid using it.

                    // if the previous snapshot was deghosted,
                    // deghost this one as well.
                    if (sh.Flags[startIndex] == SnapHistory<TSnap, TStatic>.FlagDeghosted)
                        sh.Flags[sh.LeadingIndex] = SnapHistory<TSnap, TStatic>.FlagDeghosted;
                }
            }

            // same logic as above but for second data
            // only reason we don't use a method for this is having that many method
            // calls seems like a waste.
            for (int i = 0; i < SecondData.Count; i++)
            {
                SnapHistory<TSnap, TStatic> sh = SecondData.Values[i];
                ushort last = sh.Timestamps[sh.LeadingIndex];
                ushort next = last == ushort.MaxValue ? (ushort)0 : (ushort)(last + 1);
                int startIndex = sh.LeadingIndex;

                sh.LeadingIndex++;
                if (sh.LeadingIndex == sh.Shots.Length)
                    sh.LeadingIndex = 0;

                if (sh.Timestamps[sh.LeadingIndex] != next)
                {
                    sh.Timestamps[sh.LeadingIndex] = next;
                    sh.Flags[sh.LeadingIndex] = SnapHistory<TSnap, TStatic>.FlagEmpty;
                    
                    if (sh.Flags[startIndex] == SnapHistory<TSnap, TStatic>.FlagDeghosted)
                        sh.Flags[sh.LeadingIndex] = SnapHistory<TSnap, TStatic>.FlagDeghosted;
                }
            }
        }

        /*private void AdvanceLogic(SnapHistory<T> sh)
        {
            ushort last = sh.Timestamps[sh.LeadingIndex];
            ushort next = last == ushort.MaxValue ? (ushort)0 : (ushort)(last + 1);
            int startIndex = sh.LeadingIndex;
            
            sh.LeadingIndex++;
            if (sh.LeadingIndex == sh.Shots.Length)
                sh.LeadingIndex = 0;

            if (sh.Timestamps[sh.LeadingIndex] != next)
            {
                // we haven't received the next snap yet, so destroy it and 
                // ensure that it is empty for the simulator
                sh.Timestamps[sh.LeadingIndex] = next;
                sh.Flags[sh.LeadingIndex] = SnapHistory<T>.FlagEmpty;

                // note: we don't need to actually call Clear on the snapshot
                // since the fact that it's marked with an empty flag means
                // the simulator should avoid using it.
                
                // if the previous snapshot was deghosted,
                // deghost this one as well.
                if (sh.Flags[startIndex] == SnapHistory<T>.FlagDeghosted)
                    sh.Flags[sh.LeadingIndex] = SnapHistory<T>.FlagDeghosted;
            }
        }*/

        public void LoadTimestamp(ushort timestamp)
        {
            for (int i = 0; i < FirstData.Count; i++)
                FirstData.Values[i].LoadCurrentByTimestamp(timestamp);

            for (int i = 0; i < SecondData.Count; i++)
                SecondData.Values[i].LoadCurrentByTimestamp(timestamp);
        }


        // LEAVING THIS DISCUSSION HERE FOR POSTERITY
        // ultimately we decided:
        // this can't be generalized neatly into extrap functions
        // instead, the developer / consumer of netsnapper must
        // define a simulator class that handles these questions

            // so how should the server handle advancement?
            // I was thinking that it should just supply an Action which
            // can then loop over entities and create new snapshots however
            // you desire

            // but are we getting into a mess with client extrapolate (prediction)
            // being a different process than the server's engine calculations?
            // how do we keep the central logic condensed there?

            // I guess a bigger question is: are extrapolation methods sufficient for
            // proper prediction? I guess so if they're written sufficiently
            // (e.g. if you don't include collision detection it might predict runnning
            //  through walls)
            // but maybe the real way to make things orderly is to require a engine-run
            // method from both client and server, and let the consumer game decide 
            // what is done, with the understanding that on the client side
            // missing timestamps will be filled in with prediction

            // game can then structure this in a hybrid approach where the same 
            // engine methods are called on both sides

            // and we'd have to remove the rollover extrapolation and replace it with
            // something more sophisticated here, right? that's tricky

            // and we need to account for, requesting frames that are between snapshots right?
            // are those just pure-extrapolated or do we need a system that can,
            // given a starting snapshot, calculate everything forward a given amount
            
            // on the server side, this system could be run and then new snapshots are generated 
            // based on the results
            
            // on the client side, this system is run on advancement to generate new predictive 
            // snapshots, and on frame to generate in-between frames

            // right?

            // so what we really need is some kind of advancement scaffold,
            // which has reusable memory for all the snapshots that must be loaded
            // and serves as a sandbox that prediction/calculation can be run in
            // and then the snapshots in there can be either rendered, stored
            // clientside as new silver-flags, or stored & networked by the server
            // as new gold-flags 

        // Cleanup
        public void Removed()
        {
            // Called when this snapper is removed

            // salt the earth
            FirstData.Clear();
            SecondData.Clear();

            FirstData = null;
            SecondData = null;

            FirstEntityIdToInnerId = null;
            SecondEntityIdToInnerId = null;
        }

        public void ClearEntities()
        {
            // Called when all entities of this snapper should be removed
            for (int i = FirstData.Count - 1; i >= 0; i--)
            {
                FirstEntityIdToInnerId[FirstData.Values[i].EntityId] = -1;
                FirstData.ReturnIndex(i);
            }

            for (int i = SecondData.Count - 1; i >= 0; i--)
            {
                SecondEntityIdToInnerId[SecondData.Values[i].EntityId] = -1;
                SecondData.ReturnIndex(i);
            }
        }


        // Simulant Helpers
        public void ServerSaveSimIntoCurrent(SnapHistory<TSnap, TStatic> ent)
        {  
            ent.Flags[ent.CurrentIndex] = SnapHistory<TSnap, TStatic>.FlagGold;

            if (ent.First)
            {
                NetSnapper.ServerSendDeltaAllFirst((byte)ent.EntityId, EntityType);
            }
            else
            {
                NetSnapper.ServerSendDeltaAllSecond(ent.EntityId, EntityType);
            }
        }

        public void ClientSaveSimIntoCurrent(SnapHistory<TSnap, TStatic> ent)
        {
            ent.Flags[ent.CurrentIndex] = SnapHistory<TSnap, TStatic>.FlagSilver;
        }


        
        // Entity Management Methods
        public bool ServerAddEntityFirst(TSnap initalSnap, out byte eid, bool ghostAll = true)
        {
            if (!NetSnapper.ServerRequestEntityFirst(EntityType, out eid))
            {
                return false; // NetSnapper says: no entity space available
            }

            if (FirstEntityIdToInnerId.Length <= eid)
                ExpandFirstEntityIdMap(eid);

            SnapHistory<TSnap, TStatic> h = FirstData.Request();
            h.EntityId = eid;
            h.Timestamps[0] = NetSnapper.CurrentTime;
            h.Flags[0] = SnapHistory<TSnap, TStatic>.FlagGold;
            h.LeadingIndex = 0;
            h.Shots[0] = initalSnap;

            FirstEntityIdToInnerId[eid] = h.PoolId;

            if (ghostAll)
            {
                // ghost the new entity to all players
                NetSnapper.ServerSendGhostAllFirst(eid, EntityType);
            }

            return true;
        }

        public bool ServerAddEntitySecond(TSnap initalSnap, out ushort eid, bool ghostAll = true)
        {
            if (!NetSnapper.ServerRequestEntitySecond(EntityType, out eid))
            {
                return false; // NetSnapper says: no entity space available
            }

            if (SecondEntityIdToInnerId.Length <= eid)
                ExpandSecondEntityIdMap(eid);

            SnapHistory<TSnap, TStatic> h = SecondData.Request();
            h.EntityId = eid;
            h.Timestamps[0] = NetSnapper.CurrentTime;
            h.Flags[0] = SnapHistory<TSnap, TStatic>.FlagGold;
            h.LeadingIndex = 0;
            h.Shots[0] = initalSnap;

            SecondEntityIdToInnerId[eid] = h.PoolId;

            if (ghostAll)
            {
                // ghost the new entity to all players
                NetSnapper.ServerSendGhostAllSecond(eid, EntityType);
            }

            return true;
        }

        public SnapHistory<TSnap, TStatic> GetFirstEntity(byte eid)
        {
            if (FirstEntityIdToInnerId.Length < eid
                || FirstEntityIdToInnerId[eid] == -1)
                return null;
            return FirstData.Values[FirstData.IdsToIndices[FirstEntityIdToInnerId[eid]]];
        }

        public SnapHistory<TSnap, TStatic> GetSecondEntity(ushort eid)
        {
            if (SecondEntityIdToInnerId.Length <= eid
                || SecondEntityIdToInnerId[eid] == -1)
                return null;
            return SecondData.Values[SecondData.IdsToIndices[SecondEntityIdToInnerId[eid]]];
        }
    }
}
