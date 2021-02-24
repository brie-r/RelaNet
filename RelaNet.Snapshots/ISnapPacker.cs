using RelaNet.Messages;
using System;
using System.Collections.Generic;
using System.Text;

namespace RelaNet.Snapshots
{
    public interface ISnapPacker<TSnap, TStatic, TPackInfo> 
        where TSnap : struct // entity struct
        where TStatic : struct // static data struct
        where TPackInfo : struct // pack info struct
    {
        void Clear(ref TSnap obj);

        byte PrepPackFull(TSnap obj, TStatic staticData, out TPackInfo packinfo);
        byte PrepPackDelta(TSnap obj, TSnap basis, out TPackInfo packinfo);

        void PackFull(Sent sent, TSnap obj, TStatic staticData, TPackInfo packinfo);
        void PackDelta(Sent sent, TSnap obj, TPackInfo packinfo);

        void UnpackFull(ref TSnap obj, ref TStatic staticData, byte[] blob, int start, int count);
        void UnpackDelta(ref TSnap obj, TSnap basis, byte[] blob, int start, int count);
    }
}
