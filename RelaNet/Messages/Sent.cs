using RelaNet.Sockets;
using RelaNet.Utilities;
using RelaStructures;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace RelaNet.Messages
{
    public class Sent : IPoolable
    {
        public byte[] Data;
        public int Length;

        public int Retries;
        public float RetryTimer;
        public float RetryTimerMax;
        public byte[] TargetPids;
        public int AwaitingLength; // how many targets have we added
        public float[] TargetWaits; // how long have we waited
        public byte[][] TargetAcks;

        public ushort[] TargetMessageId;
        public bool HasMessageId;
        public int PoolIndex;
        public bool IsImmediate;
        public bool IsOrdered;


        public ushort[] FastOrderValues; // per player, order index at time sent was made
        public BitArray FastOrderSets; // per player, whether fast order value has been set
        public bool IsFastOrdered;

        public bool Finalized; // has it been sent to at least one target?

        public Action<byte, ushort, bool> HandshakeCallback;

        public const int AckPosition = 5; // the index of the handshake within a sent's buffer
        public const int MidPosition = 2; // the index of the message id within a sent's buffer
        public const int PidPosition = 0; // ''               player id  ''
        public const int TargetPidPosition = 1; // ''               player id  ''
        public const int SpecialPosition = 4; // ''           special id ''

        public const int EmptySizeWithAck = AckPosition + 12;
        public const int OrderValuePosition = AckPosition + 12;
        public const int EmptySizeWithOrderValue = OrderValuePosition + 2;


        public Sent()
        {
            Data = new byte[UdpClientExtensions.MaxUdpSize];
            Length = 0;

            Retries = 0;
            RetryTimer = 0;
            RetryTimerMax = 0;
            TargetPids = new byte[2];
            AwaitingLength = 0;
            TargetWaits = new float[2];

            TargetMessageId = new ushort[2];
            HasMessageId = false;
            PoolIndex = -1;
            IsImmediate = false;
            IsOrdered = false;

            TargetAcks = new byte[8][];
            for (int i = 0; i < TargetAcks.Length; i++)
                TargetAcks[i] = new byte[12];

            FastOrderValues = new ushort[8];
            FastOrderSets = new BitArray(256);
            IsFastOrdered = false;

            HandshakeCallback = null;

            Finalized = false;
        }

        public int AddTarget(byte pid, ushort mid)
        {
            if (TargetPids.Length <= AwaitingLength)
            {
                // expand the targetPids array
                byte[] ntp = new byte[TargetPids.Length * 2];
                for (int i = 0; i < TargetPids.Length; i++)
                    ntp[i] = TargetPids[i];
                TargetPids = ntp;
            }
            if (TargetWaits.Length <= AwaitingLength)
            {
                // expand targetWaits array
                float[] ntw = new float[TargetWaits.Length * 2];
                for (int i = 0; i < TargetWaits.Length; i++)
                    ntw[i] = TargetWaits[i];
                TargetWaits = ntw;
            }
            if (TargetMessageId.Length <= AwaitingLength)
            {
                ushort[] nmid = new ushort[TargetMessageId.Length * 2];
                for (int i = 0; i < TargetMessageId.Length; i++)
                    nmid[i] = TargetMessageId[i];
                TargetMessageId = nmid;
            }
            if (TargetAcks.Length <= AwaitingLength)
            {
                // we need to expand the arrays
                byte[][] nacks = new byte[TargetAcks.Length * 2][];
                for (int i = 0; i < TargetAcks.Length; i++)
                {
                    nacks[i] = TargetAcks[i];
                }
                for (int i = TargetAcks.Length; i < nacks.Length; i++)
                    nacks[i] = new byte[12];

                TargetAcks = nacks;
            }
            TargetPids[AwaitingLength] = pid;
            TargetWaits[AwaitingLength] = 0;
            TargetMessageId[AwaitingLength] = mid;
            int index = AwaitingLength;
            AwaitingLength++;
            return index;
        }

        public void RemoveTargetIndex(int index, bool skiphandshake)
        {
             if (HandshakeCallback != null)
                HandshakeCallback(TargetPids[index], TargetMessageId[index], !skiphandshake);

            if (AwaitingLength <= 1)
            {
                AwaitingLength = 0;
                return;
            }

            AwaitingLength--;
            TargetPids[index] = TargetPids[AwaitingLength];
        }

        public int GetTargetIndex(byte pid)
        {
            int ackIndex = -1;
            for (int i = 0; i < TargetPids.Length; i++)
            {
                if (TargetPids[i] == pid)
                {
                    ackIndex = i;
                    break;
                }
            }
            return ackIndex;
        }

        public void LoadAckFromPid(byte pid)
        {
            // load the given ack out of our Acks list and put it
            // into Data at the appropriate offset
            int ackIndex = -1;
            for (int i = 0; i < TargetPids.Length; i++)
            {
                if (TargetPids[i] == pid)
                {
                    ackIndex = i;
                    break;
                }
            }

            LoadAckFromIndex(ackIndex);
        }

        public void LoadAckFromIndex(int targetIndex)
        { 
            if (targetIndex != -1)
            {
                // load the message id as well
                Bytes.WriteUShort(Data, TargetMessageId[targetIndex], MidPosition);

                byte[] nack = TargetAcks[targetIndex];
                Buffer.BlockCopy(nack, 0, Data, AckPosition, 12);
            }
            else
                throw new Exception("Tried to load Ack for player not included in Sent");
        }

        public void AddFastOrderValue(byte pid, ushort value)
        {
            while (FastOrderValues.Length <= pid)
            {
                // must expand
                ushort[] nfov = new ushort[FastOrderValues.Length * 2];
                for (int i = 0; i < FastOrderValues.Length; i++)
                    nfov[i] = FastOrderValues[i];
                FastOrderValues = nfov;
            }
            FastOrderValues[pid] = value;
            FastOrderSets[pid] = true;
        }

        public void LoadFastOrderValue(byte pid)
        {
            Bytes.WriteUShort(Data, FastOrderValues[pid], OrderValuePosition);
        }

        public void Clear()
        {
            Length = 0;
            HasMessageId = false;
            AwaitingLength = 0;
            Retries = 0;
            IsImmediate = false;
            IsOrdered = false;
            Finalized = false;
            if (IsFastOrdered)
                FastOrderSets.SetAll(false);
            IsFastOrdered = false;
            HandshakeCallback = null;
        }

        public void SetPoolIndex(int index)
        {
            PoolIndex = index;
        }

        public int GetPoolIndex()
        {
            return PoolIndex;
        }


        #region Writing Methods
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByte(byte b)
        {
            Data[Length] = b; Length++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteSByte(sbyte b)
        {
            Bytes.WriteSByte(Data, b, Length); Length++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUShort(ushort u)
        {
            Bytes.WriteUShort(Data, u, Length); Length += 2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt(int i)
        {
            Bytes.WriteInt(Data, i, Length); Length += 4;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(string s)
        {
            Length += Bytes.WriteString(Data, s, Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteFloat(float f)
        {
            Bytes.WriteFloat(Data, f, Length); Length += 4;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDouble(double d)
        {
            Bytes.WriteDouble(Data, d, Length); Length += 8;
        }
        #endregion
    }
}
