using Microsoft.VisualStudio.TestTools.UnitTesting;
using RelaNet.Sockets;
using System.Net;

namespace RelaNet.UT
{
    [TestClass]
    public class ServerTests
    {
        [TestMethod]
        public void SimpleServerTest()
        {
            TestEnvironment tenv = new TestEnvironment(2, (e) => { });

            tenv.BeginChallenge(
                0,
                (e) => {
                    Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                },
                "client1", "");

            // let the environment tick and verify the connection is confirmed
            tenv.TickRepeat(6, 100);
            Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 2);
            Assert.IsTrue(tenv.Clients[0].ClientConnected);

            tenv.BeginChallenge(
                1,
                (e) => {
                    Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                },
                "client2", "");

            // let the environment tick and verify the connection is confirmed
            tenv.TickRepeat(6, 100);
            Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 3);
            Assert.IsTrue(tenv.Clients[0].ClientConnected);
            Assert.IsTrue(tenv.Clients[1].ClientConnected);

            // verify the two clients can see eachother as well
            Assert.IsTrue(tenv.Clients[0].PlayerInfos.Count == 3);
            Assert.IsTrue(tenv.Clients[1].PlayerInfos.Count == 3);
            bool foundc1 = false;
            bool foundc2 = false;
            for (int i = 0; i < tenv.Clients[0].PlayerInfos.Count; i++)
            {
                if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client1")
                    foundc1 = true;
                if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client2")
                    foundc2 = true;
            }
            Assert.IsTrue(foundc1);
            Assert.IsTrue(foundc2);

            foundc1 = false;
            foundc2 = false;
            for (int i = 0; i < tenv.Clients[1].PlayerInfos.Count; i++)
            {
                if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client1")
                    foundc1 = true;
                if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client2")
                    foundc2 = true;
            }
            Assert.IsTrue(foundc1);
            Assert.IsTrue(foundc2);
        }

        [TestMethod]
        public void SimpleServerMidLatencyTest()
        {
            int seed = 50;
            for (int tests = 0; tests < 100; tests++)
            {
                TestEnvironment tenv = new TestEnvironment(2, (e) => { });

                tenv.SetSocketRandom(seed);
                seed += 392;
                tenv.SetLatency(40, 80);
                tenv.SetDropChance(0.1);

                tenv.BeginChallenge(
                    0,
                    (e) =>
                    {
                        Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                    },
                    "client1", "");

                // let the environment tick and verify the connection is confirmed
                tenv.TickRepeat(11, 200);
                Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 2);
                Assert.IsTrue(tenv.Clients[0].ClientConnected);

                tenv.BeginChallenge(
                    1,
                    (e) =>
                    {
                        Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                    },
                    "client2", "");

                // let the environment tick and verify the connection is confirmed
                tenv.TickRepeat(11, 200);
                Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 3);
                Assert.IsTrue(tenv.Clients[0].ClientConnected);
                Assert.IsTrue(tenv.Clients[1].ClientConnected);

                // verify the two clients can see eachother as well
                Assert.IsTrue(tenv.Clients[0].PlayerInfos.Count == 3);
                Assert.IsTrue(tenv.Clients[1].PlayerInfos.Count == 3);
                bool foundc1 = false;
                bool foundc2 = false;
                for (int i = 0; i < tenv.Clients[0].PlayerInfos.Count; i++)
                {
                    if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client1")
                        foundc1 = true;
                    if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client2")
                        foundc2 = true;
                }
                Assert.IsTrue(foundc1);
                Assert.IsTrue(foundc2);

                foundc1 = false;
                foundc2 = false;
                for (int i = 0; i < tenv.Clients[1].PlayerInfos.Count; i++)
                {
                    if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client1")
                        foundc1 = true;
                    if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client2")
                        foundc2 = true;
                }
                Assert.IsTrue(foundc1);
                Assert.IsTrue(foundc2);
            }
        }

        [TestMethod]
        public void SimpleServerHighLatencyTest()
        {
            int seed = 50;
            for (int tests = 0; tests < 100; tests++)
            {
                TestEnvironment tenv = new TestEnvironment(2, (e) => { });

                tenv.SetSocketRandom(seed);
                seed += 392;
                tenv.SetLatency(100, 200);
                tenv.SetDropChance(0.3);

                tenv.BeginChallenge(
                    0,
                    (e) =>
                    {
                        Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                    },
                    "client1", "");

                // let the environment tick and verify the connection is confirmed
                tenv.TickRepeat(11, 300);
                Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 2);
                Assert.IsTrue(tenv.Clients[0].ClientConnected);

                tenv.BeginChallenge(
                    1,
                    (e) =>
                    {
                        Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                    },
                    "client2", "");

                // let the environment tick and verify the connection is confirmed
                tenv.TickRepeat(11, 300);
                Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 3);
                Assert.IsTrue(tenv.Clients[0].ClientConnected);
                Assert.IsTrue(tenv.Clients[1].ClientConnected);

                // verify the two clients can see eachother as well
                Assert.IsTrue(tenv.Clients[0].PlayerInfos.Count == 3);
                Assert.IsTrue(tenv.Clients[1].PlayerInfos.Count == 3);
                bool foundc1 = false;
                bool foundc2 = false;
                for (int i = 0; i < tenv.Clients[0].PlayerInfos.Count; i++)
                {
                    if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client1")
                        foundc1 = true;
                    if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client2")
                        foundc2 = true;
                }
                Assert.IsTrue(foundc1);
                Assert.IsTrue(foundc2);

                foundc1 = false;
                foundc2 = false;
                for (int i = 0; i < tenv.Clients[1].PlayerInfos.Count; i++)
                {
                    if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client1")
                        foundc1 = true;
                    if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client2")
                        foundc2 = true;
                }
                Assert.IsTrue(foundc1);
                Assert.IsTrue(foundc2);
            }
        }

        [TestMethod]
        public void LongUptimeServerTest()
        {
            TestEnvironment tenv = new TestEnvironment(2, (e) => { });

            tenv.BeginChallenge(
                0,
                (e) => {
                    Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                },
                "client1", "");

            // let the environment tick and verify the connection is confirmed
            tenv.TickRepeat(6, 100);
            Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 2);
            Assert.IsTrue(tenv.Clients[0].ClientConnected);

            tenv.BeginChallenge(
                1,
                (e) => {
                    Assert.AreEqual(NetServer.EChallengeResponse.ACCEPT, e);
                },
                "client2", "");

            // let the environment tick and verify the connection is confirmed
            tenv.TickRepeat(6, 100);
            Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 3);
            Assert.IsTrue(tenv.Clients[0].ClientConnected);
            Assert.IsTrue(tenv.Clients[1].ClientConnected);

            // verify the two clients can see eachother as well
            Assert.IsTrue(tenv.Clients[0].PlayerInfos.Count == 3);
            Assert.IsTrue(tenv.Clients[1].PlayerInfos.Count == 3);
            bool foundc1 = false;
            bool foundc2 = false;
            for (int i = 0; i < tenv.Clients[0].PlayerInfos.Count; i++)
            {
                if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client1")
                    foundc1 = true;
                if (tenv.Clients[0].PlayerInfos.Values[i].Name == "client2")
                    foundc2 = true;
            }
            Assert.IsTrue(foundc1);
            Assert.IsTrue(foundc2);

            foundc1 = false;
            foundc2 = false;
            for (int i = 0; i < tenv.Clients[1].PlayerInfos.Count; i++)
            {
                if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client1")
                    foundc1 = true;
                if (tenv.Clients[1].PlayerInfos.Values[i].Name == "client2")
                    foundc2 = true;
            }
            Assert.IsTrue(foundc1);
            Assert.IsTrue(foundc2);

            // now let the server tick for a long time
            tenv.TickRepeat(20, 15000);

            // check that the clients have not abandoned
            Assert.IsTrue(tenv.ServerHost.PlayerInfos.Count == 3);
        }
    }
}
