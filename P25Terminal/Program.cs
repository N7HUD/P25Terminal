using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;

namespace P25Terminal
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            string callsign = "";
            string address = "";

            try
            {
                string[] configFile = File.ReadAllLines("settings.cfg");
                foreach (string config in configFile)
                {
                    if (config.Contains('='))
                    {
                        string key = config.Split('=')[0];
                        string value = config.Split("=")[1];

                        switch (key)
                        {
                            case "callsign":
                                {
                                    callsign = value.Trim();
                                }
                                break;
                            case "address":
                                {
                                    address = value.Trim();
                                }
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Config file is missing, aborting");
                return;
            }

            if(callsign == "" || address == "")
            {
                Console.WriteLine("Config is missing, aborting");
                return;
            }

            NetworkEndpoint nep = new NetworkEndpoint(callsign, address);

            nep.Start();

            while (true)
            {
                string msg = "";
                msg = Console.ReadLine();

                if (msg == "filetest")
                {
                    //Screenshot 2026-07-05 193501_compressed.jpg
                    //NetworkFile nf = new NetworkFile(@"C:\Users\Radiian\Documents\lipsum15k.txt");
                    NetworkFile nf = new NetworkFile(@"C:\Users\Radiian\Pictures\Screenshot 2026-07-05 193501_compressed.jpg");

                    FileInfo info = nf.GetInfo();

                    Debug.WriteLine($"File info reports {info.fileParts} file parts");

                    nep.SendFile(nf);

                    Debug.WriteLine("File send complete");
                    
                    //nep.resend = false;
                    //FileStream fs = File.Open(@"C:\Users\Radiian\Documents\lipsum15k.txt", FileMode.Open);
                    //long len = fs.Length;
                    //byte[] buf = new byte[len];

                    //fs.Read(buf, 0, (int)len);
                    //fs.Close();
                    //int packetSize = 2048;

                    //long parts = len / packetSize;
                    //for (int i = 0; i < parts; ++i)
                    //{
                    //    byte[] tmpbuf = new byte[packetSize];
                    //    Array.Copy(buf, i * packetSize, tmpbuf, 0, packetSize);
                    //    Packet p = nep.Send(tmpbuf);
                    //    while(!nep.IsPackedAcked(p.Id))
                    //    {
                    //        Thread.Sleep(500);
                    //    }
                    //}
                    //long sent = parts * packetSize;
                    //if(sent < len)
                    //{
                    //    long dif = len - sent;
                    //    byte[] tmpbuf = new byte[dif];

                    //    Array.Copy(buf, sent, tmpbuf, 0, dif);
                    //    Packet p = nep.Send(tmpbuf);
                    //    while (!nep.IsPackedAcked(p.Id))
                    //    {
                    //        Thread.Sleep(500);
                    //    }
                    //}

                    //Console.WriteLine("File send has completed");


                }
                else
                {
                    nep.resend = true;
                    nep.Send(msg);
                }
            }
        }
    }
}
