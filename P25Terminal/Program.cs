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

                if (msg.StartsWith("echo:"))
                {
                    string newmsg = msg.Split(":")[1];
                    nep.Send(newmsg, true);
                }
                else
                {
                    nep.Send(msg);
                }
            }
        }
    }
}
