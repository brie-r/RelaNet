using RelaNet.Snapshots.Basic2d;
using RelaNet.UT;
using System;
using System.Collections.Generic;
using System.Text;

namespace RelaNet.Snapshots.UT.Basic2d
{
    public class SnapBasic2dEnvironment
    {
        public TestEnvironment Tenv;

        public List<NetExecutorSnapper> NetSnappers 
            = new List<NetExecutorSnapper>();

        public List<Snapper<NentBasic2d, NentStaticBasic2d, PackerBasic2d, PackInfoBasic2d>> Nents 
            = new List<Snapper<NentBasic2d, NentStaticBasic2d, PackerBasic2d, PackInfoBasic2d>>();

        public List<SnapInputManager<InputBasic2d, InputPackerBasic2d>> Inputs 
            = new List<SnapInputManager<InputBasic2d, InputPackerBasic2d>>();

        public List<SimulatorBasic2d> Sims = new List<SimulatorBasic2d>();
        

        public SnapBasic2dEnvironment(int clientCount)
        {
            Tenv = TestEnvironment.AutoConnected(clientCount,
                (serv) =>
                {
                    NetExecutorSnapper netSnap = new NetExecutorSnapper();
                    NetSnappers.Add(netSnap);
                    serv.AddExecutor(netSnap);

                    SnapInputManager<InputBasic2d, InputPackerBasic2d> input =
                        InputManagerBasic2d.Make();
                    netSnap.AddInputManager(input);
                    Inputs.Add(input);

                    Snapper<NentBasic2d, NentStaticBasic2d, PackerBasic2d, PackInfoBasic2d> nent =
                        SnapperBasic2d.Make();
                    netSnap.AddSnapper(nent);
                    Nents.Add(nent);

                    SimulatorBasic2d sim = new SimulatorBasic2d(input, nent);
                    netSnap.LoadSimulator(sim);
                    Sims.Add(sim);
                });
        }

        public void Activate()
        {
            for (int i = 0; i < NetSnappers.Count; i++)
                NetSnappers[i].Activate();
        }

        public void Deactivate()
        {
            for (int i = 0; i < NetSnappers.Count; i++)
                NetSnappers[i].Deactivate();
        }

        public bool AddEntityFirst(NentBasic2d initialSnap, out byte eid)
        {
            return Nents[0].ServerAddEntityFirst(initialSnap, out eid);
        }

        public bool AddEntitySecond(NentBasic2d initialSnap, out ushort eid)
        {
            return Nents[0].ServerAddEntitySecond(initialSnap, out eid);
        }

        public void Tick(float elapsedms)
        {
            Tenv.ServerHost.Tick(elapsedms);
            for (int i = 0; i < Tenv.Clients.Length; i++)
            {
                // send default input
                Inputs[i + 1].WriteInput(new InputBasic2d());

                Tenv.Clients[i].Tick(elapsedms);
            }
        }

        public void TickRepeat(float elapsedms, int times)
        {
            for (int i = 0; i < times; i++)
                Tick(elapsedms);
        }

        public void StandardTick()
        {
            TickRepeat(6, 100);
        }

        public void FastTick()
        {
            TickRepeat(24, 5);
        }
    }
}
