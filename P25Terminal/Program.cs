using System;

namespace P25Terminal
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");
            NetworkEndpoint nep = new NetworkEndpoint();

            nep.Start();

            while (true)
            {
                string msg = "";
                msg = Console.ReadLine();

                if (msg == "filetest")
                {
                    nep.resend = false;
                    FileStream fs = File.Open(@"C:\Users\Radiian\Documents\lipsum15k.txt", FileMode.Open);
                    long len = fs.Length;
                    byte[] buf = new byte[len];

                    fs.Read(buf, 0, (int)len);
                    fs.Close();
                    int packetSize = 2048;

                    long parts = len / packetSize;
                    for (int i = 0; i < parts; ++i)
                    {
                        byte[] tmpbuf = new byte[packetSize];
                        Array.Copy(buf, i * packetSize, tmpbuf, 0, packetSize);
                        Packet p = nep.Send(tmpbuf);
                        while(!nep.IsPackedAcked(p.Id))
                        {
                            Thread.Sleep(500);
                        }
                    }
                    long sent = parts * packetSize;
                    if(sent < len)
                    {
                        long dif = len - sent;
                        byte[] tmpbuf = new byte[dif];

                        Array.Copy(buf, sent, tmpbuf, 0, dif);
                        Packet p = nep.Send(tmpbuf);
                        while (!nep.IsPackedAcked(p.Id))
                        {
                            Thread.Sleep(500);
                        }
                    }

                    Console.WriteLine("File send has completed");


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
