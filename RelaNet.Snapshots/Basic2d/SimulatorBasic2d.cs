﻿using RelaNet.Utilities;
using System;
using System.Collections.Generic;
using System.Text;

namespace RelaNet.Snapshots.Basic2d
{
    public class SimulatorBasic2d : ISnapSimulator
    {
        public NetServer Server;
        public SnapInputManager<InputBasic2d, InputPackerBasic2d> Input;
        public Snapper<NentBasic2d, PackerBasic2d, PackInfoBasic2d> Nents;

        private NetExecutorSnapper NetSnapper;

        private byte[] PlayerEntityIds = new byte[byte.MaxValue];

        public const ushort NENT_PLAYEROBJ = 1;

        // note: choosing to hardcode this value. If you wanted e.g.
        // each player to have a different speed, you would need to
        // add a speed float to the NentBasic2d struct and reference
        // that instead.
        public float PlayerSpeed = 0.3f; // per ms
        // when a player gives input A, they begin a dash that lasts
        // this long. We're using [Free1] to hold the timer value
        // in this example.
        public float DashTimerMax = 500f; // ms
        // afterwards, [Free1] is set to -[DashCooldownMax] (note the
        // negative sign) and it ticks back up to 0. When it hits 0 
        // the dash is available again.
        public float DashCooldownMax = 1000f; // ms
        public float DashSpeed = 0.9f; // per ms

        public SimulatorBasic2d(
            SnapInputManager<InputBasic2d, InputPackerBasic2d> input,
            Snapper<NentBasic2d, PackerBasic2d, PackInfoBasic2d> nents)
        {
            Input = input;
            Nents = nents;
        }

        public void Loaded(NetExecutorSnapper snapper)
        {
            NetSnapper = snapper;
            Server = NetSnapper.Server;
        }


        // Advance Methods
        public void ClientAdvance(int times, float tickms)
        {
            for (int t = 0; t < times; t++)
            {
                for (int i = 0; i < Nents.FirstData.Count; i++)
                {
                    SnapHistory<NentBasic2d> h = Nents.FirstData.Values[i];

                    if (h.PrevFlag == SnapHistory<NentBasic2d>.FlagEmpty
                        || h.PrevFlag == SnapHistory<NentBasic2d>.FlagDeghosted)
                        continue; // if the prev flag is empty, this entity does 
                                  // not exist yet

                    // check to see if this is one of our player objects,
                    // if so, we resimulate it according to our inputs
                    bool skipAdvancement = false;
                    if (h.Prev.Id2 == NENT_PLAYEROBJ)
                    {                        
                        // store the player entities so we know which entity belongs
                        // to which player
                        PlayerEntityIds[h.Prev.Id1] = (byte)h.EntityId;

                        // we'll only have inputs if it is our player object
                        // since we don't receive inputs for other players
                        if (h.EntityId == Server.OurPlayerId)
                        {
                            // if we have no inputs, skip advancement will be false
                            // and we'll do regular simulation on this object (see below)
                            skipAdvancement = InputChecker(h, (byte)h.EntityId);
                        }
                    }
                    
                    if (!skipAdvancement)
                    {
                        if (h.CurrentFlag == SnapHistory<NentBasic2d>.FlagGold)
                            continue; // if we already have a gold from the server,
                                      // there is no purpose in resimulating.

                        // we know the prev snapshot must be gold or silver, or we
                        // wouldn't be here right now. So now we just need to see
                        // what the next snapshot is.
                        
                        // if we have a gold snapshot ahead of us, we can just 
                        // interpolate between Prev and Next, which is less costly.
                        // if not, we need to Advance from Prev to Current

                        // check Next
                        if (h.NextFlag != SnapHistory<NentBasic2d>.FlagGold)
                        {
                            // we don't have gold ahead of us, so we must 
                            // simulate the current snapshot from previous
                            AdvanceLogic(ref h.Prev, NetSnapper.TickMSTarget);                        }
                        else
                        {
                            // we can interpolate since the next flag is gold
                            InterpolateSnapLogic(h);
                        }
                    }

                    // if we have gold we would have already `continue;`d by now
                    // so it's safe to save as silver here
                    Nents.ClientSaveSimPrevIntoCurrent(h);
                }

                for (int i = 0; i < Nents.SecondData.Count; i++)
                {
                    SnapHistory<NentBasic2d> h = Nents.SecondData.Values[i];

                    if (h.PrevFlag == SnapHistory<NentBasic2d>.FlagEmpty
                        || h.PrevFlag == SnapHistory<NentBasic2d>.FlagDeghosted)
                        continue;

                    if (h.CurrentFlag == SnapHistory<NentBasic2d>.FlagGold)
                        continue; 

                    if (h.NextFlag != SnapHistory<NentBasic2d>.FlagGold)
                    {
                        AdvanceLogic(ref h.Prev, NetSnapper.TickMSTarget);
                    }
                    else
                    {
                        InterpolateSnapLogic(h);
                    }

                    Nents.ClientSaveSimPrevIntoCurrent(h);
                }
                
                // load next
                NetSnapper.AdvanceSimulateTimestamp();
            }

            // now advance to TickMS
            for (int i = 0; i < Nents.FirstData.Count; i++)
            {
                SnapHistory<NentBasic2d> h = Nents.FirstData.Values[i];

                if (h.PrevFlag == SnapHistory<NentBasic2d>.FlagEmpty
                    || h.PrevFlag == SnapHistory<NentBasic2d>.FlagDeghosted)
                    continue; // if the prev flag is empty, this entity does 
                              // not exist yet
                
                if (h.CurrentFlag == SnapHistory<NentBasic2d>.FlagGold)
                {
                    // we have a gold flag, so we can interpolate
                    InterpolateLogic(h, tickms);
                }
                else
                {
                    // otherwise we must simulate
                    AdvanceLogic(ref h.Current, tickms);
                }
            }

            // now Current is populated with the latest snapshots
            // adjusted forward to meet the current tickMS
            // this can be rendered by the client
        }



        public void ServerAdvance()
        {
            // handle inputs
            /*for (int i = 0; i < Server.PlayerInfos.Count; i++)
            {
                byte pid = Server.PlayerInfos.Values[i].PlayerId;
                if (pid == 0)
                    continue; // server doesn't have inputs

                InputChecker(pid);
            }*/

            for (int i = 0; i < Nents.FirstData.Count; i++)
            {
                SnapHistory<NentBasic2d> h = Nents.FirstData.Values[i];

                if (h.PrevFlag != SnapHistory<NentBasic2d>.FlagGold)
                    continue; // if the prevflag is not gold,
                              // the entity does not exist at this timestamp
                              // so we don't bother simulating it yet

                // now we advance the snapshot forward to the current point in
                // time, and then save it
                if (h.Prev.Id2 == NENT_PLAYEROBJ)
                {
                    // store the player entities so we know which entity belongs
                    // to which player
                    PlayerEntityIds[h.Prev.Id1] = (byte)h.EntityId;

                    // if we have no inputs, do regular advancement
                    if (!InputChecker(h, (byte)h.EntityId))
                        AdvanceLogic(ref h.Prev, NetSnapper.TickMSTarget); 
                }
                else
                {
                    // if this were client, we'd check the flag of the next.
                    // as it is, just create the next from the current
                    AdvanceLogic(ref h.Prev, NetSnapper.TickMSTarget);
                }

                Nents.ServerSaveSimPrevIntoCurrent(h);
            }
            
            for (int i = 0; i < Nents.SecondData.Count; i++)
            {
                SnapHistory<NentBasic2d> h = Nents.SecondData.Values[i];

                if (h.PrevFlag != SnapHistory<NentBasic2d>.FlagGold)
                    continue;

                // not checking input here because second entities should not
                // be player objects. obviously you could check input here
                // if you wanted to make second entities inputable for
                // some reason (if you needed a lot of them?)
                AdvanceLogic(ref h.Prev, NetSnapper.TickMSTarget);

                Nents.ServerSaveSimPrevIntoCurrent(h);
            }
        }



        private void AdvanceLogic(ref NentBasic2d snap, float delta)
        {
            if (snap.Id2 == NENT_PLAYEROBJ)
            {
                // some special considerations for the playerobject
                if (snap.Free1 > 0)
                {
                    // if Free1 is over 0, we're in the middle of a dash
                    // this means we're moving quickly in the direction
                    // of our rotation

                    // we're expecting the client to provide rotation in
                    // radians, for the record
                    snap.XVel = DashSpeed * RMathF.Cos(snap.Rot);
                    snap.YVel = DashSpeed * RMathF.Sin(snap.Rot);

                    // reduce the dash timer 
                    snap.Free1 -= delta;
                    // if we cross 0, set up the cooldown timer
                    if (snap.Free1 <= 0)
                        snap.Free1 = -DashCooldownMax;
                    
                    // note: something we're not really handling here
                    // is that on the tick that the dash ends, we may 
                    // get a few ms of "extra" dash b/c if it has say
                    // 10ms remaining and we have 16ms in the tick, 
                    // you're dashing for the full 16ms even though
                    // you should only have 10ms of dash.

                    // you could probably fix this issue if precision
                    // matters in your use case. but for this demo
                    // I think it's outside the scope
                }
                else if (snap.Free1 < 0)
                {
                    // if we're negative, we're on cooldown
                    // count back up to 0, when we get to 0
                    // the dash is available again.
                    snap.Free1 += delta;
                    if (snap.Free1 >= 0)
                        snap.Free1 = 0;
                }
            }

            snap.X += snap.XVel * delta;
            snap.Y += snap.YVel * delta;
        }

        private void InterpolateSnapLogic(SnapHistory<NentBasic2d> h)
        {
            // interpolates from Prev to Next to create Current

            // always inherit ids from previous
            //h.Prev.Id1 = h.Prev.Id1;
            //h.Prev.Id2 = h.Prev.Id2;

            // pos/vel is easy, just average them
            h.Prev.X = (h.Next.X + h.Prev.X) / 2f;
            h.Prev.Y = (h.Next.Y + h.Prev.Y) / 2f;
            h.Prev.XVel = (h.Next.XVel + h.Prev.XVel) / 2f;
            h.Prev.YVel = (h.Next.YVel + h.Prev.YVel) / 2f;

            // in our case, we use Free1 as a timer, so it makes 
            // sense to average this as well. May not be the case
            // if Free1 is used for a different kind of value
            h.Prev.Free1 = (h.Next.Free1 + h.Prev.Free1) / 2f;

            // rotation is more complicated to find the midpoint
            h.Prev.Rot = RMathF.AngleMidpoint(h.Prev.Rot, h.Next.Rot);
        }

        private void InterpolateLogic(SnapHistory<NentBasic2d> h, float delta)
        {
            // interpolate from Current forward delta ms
            float tickpercent = delta / NetSnapper.TickMSTarget;
            float invtickpercent = 1.0f - tickpercent;

            // we don't adjust IDs at all

            // pos/vel is simple, just blend
            h.Current.X = (h.Current.X * invtickpercent) + (h.Next.X * tickpercent);
            h.Current.Y = (h.Current.Y * invtickpercent) + (h.Next.Y * tickpercent);
            h.Current.XVel = (h.Current.XVel * invtickpercent) + (h.Next.XVel * tickpercent);
            h.Current.YVel = (h.Current.YVel * invtickpercent) + (h.Next.YVel * tickpercent);

            // it makes sense to blend Free1 as well since we just
            // use it as a timer, but in other cases this might not 
            // be appropriate
            h.Current.Free1 = (h.Current.Free1 * invtickpercent) + (h.Next.Free1 * tickpercent);

            // rotation is more complicated to blend
            h.Current.Rot = RMathF.AngleBlend(h.Current.Rot, h.Next.Rot, tickpercent);
        }

        // returns false if we have no inputs
        private bool InputChecker(SnapHistory<NentBasic2d> h, byte pid)
        {
            InputBasic2d[] actions = Input.GetPlayerInputs(pid,
                    out int index, out int count,
                    out ushort[] timestamps, out float[] tickms);

            if (count == 0)
                return false;

            ushort simstamp = NetSnapper.SimulateTimestamp;

            // timestamps are ordered from oldest --> latest
            // loop through seeking our current timestamp
            float lastms = 0;
            InputBasic2d lastAction = new InputBasic2d();
            bool hasLastAction = false;
            for (int i = 0; i < count; i++)
            {
                // if timestamps[index] is later than our timestamp, stop
                // if timestamps[index] is earlier than our timestamp, keep going
                // if timestamps[index] is equal to our timestamp, process it
                if (timestamps[index] == simstamp)
                {
                    // process it
                    if (hasLastAction)
                    {
                        InputLogic(lastAction, h, pid, tickms[index] - lastms);
                    }
                    hasLastAction = true;
                    lastms = tickms[index];
                    lastAction = actions[index];
                }
                else if (simstamp >= ushort.MaxValue / 2)
                {
                    if (timestamps[index] > simstamp
                        || timestamps[index] < simstamp - (ushort.MaxValue / 2))
                    {
                        // later
                        break;
                    }
                }
                else
                {
                    if (timestamps[index] > simstamp
                        && timestamps[index] < simstamp + (ushort.MaxValue / 2))
                    {
                        // later
                        break;
                    }
                }

                index++;
                if (index >= actions.Length)
                    index -= actions.Length;
            }

            // process the last action, now that we're done
            if (hasLastAction)
            {
                // we use the tickrate here bbecause this action stretches until the end
                // of the tick, since there are no subsequent actions in this tick
                InputLogic(lastAction, h, pid, NetSnapper.TickMSTarget - lastms);
            }

            return true;
        }

        private void InputLogic(InputBasic2d action, SnapHistory<NentBasic2d> h, byte pid, float delta)
        {
            // process the inputs for this action 
            h.Prev.XVel = RMathF.Clamp(action.Horizontal, -1f, 1f) * PlayerSpeed;
            h.Prev.YVel = RMathF.Clamp(action.Vertical, -1f, 1f) * PlayerSpeed;

            // set our rotation, but only if we're not mid-dash
            if (h.Prev.Free1 <= 0)
                h.Prev.Rot = action.Rotation;

            // dash action
            if ((action.Inputs & InputBasic2d.INPUT_A) != 0
                && h.Prev.Free1 == 0)
            {
                // if input A is pressed, dash forward according to rotation
                // we use Free1 to store the dash timer. We can only begin a
                // dash if Free1 is equal to 0 (e.g. dash is over).
                h.Prev.Free1 = DashTimerMax;

                // we don't need to set XVel/YVel here because this is done
                // in AdvanceLogic
            }

            // finally, do AdvanceLogic over the delta window
            AdvanceLogic(ref h.Prev, delta);
        }

        
    }
}
