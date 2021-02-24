using System;
using System.Collections.Generic;
using System.Text;

namespace RelaNet.Snapshots.Basic2d
{
    public class SnapperBasic2d
    {
        public static Snapper<NentBasic2d, NentStaticBasic2d, PackerBasic2d, PackInfoBasic2d> Make(
            int firstWindowLength = 64, int secondWindowLength = 32)
        {
            return new Snapper<NentBasic2d, NentStaticBasic2d, PackerBasic2d, PackInfoBasic2d>(
                firstWindowLength, secondWindowLength);
        }
    }
}
