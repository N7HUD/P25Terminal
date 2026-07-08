using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace P25Terminal
{
    public enum PacketType : uint
    {
        BAD_PACKET = 0,
        CLIENT_CONNECT = 1010,
        CLIENT_CONNECT_ACK = 1020,
        CLIENT_DISCONNECT = 1234,
        PACKET_ACK = 3020,
        RESEND_REQUEST = 3025,
        GENERIC_PAYLOAD = 4103,
        ECHO_REQUEST = 4104,
    }

    struct SentPacket
    {
        public Packet p;
        public long timestamp;
        public int retries;
    }


    public class Packet
    {
        public Packet() { }

        public Packet(byte[] bytes)
        {
            if (bytes.Length < 22)
            {
                this.Type = PacketType.BAD_PACKET;
            }
            else
            {
                this.Id = BitConverter.ToUInt32(bytes, 0);
                this.Type = (PacketType)BitConverter.ToUInt32(bytes, 4);
                Array.Copy(bytes, 8, this.Callsign, 0, 10);
                this.PayloadLength = BitConverter.ToInt32(bytes, 18);

                if (this.PayloadLength > 0)
                {
                    this.Payload = new byte[this.PayloadLength];
                    Array.Copy(bytes, 22, this.Payload, 0, this.PayloadLength);
                }
            }
        }

        public void SetCallsign(string callsign)
        {
            for (int i = 0; i < 10 && i < callsign.Length; i++)
            {
                this.Callsign[i] = callsign[i];
            }
        }

        public byte[] GetBytes()
        {
            long size = 4 + 4 + (sizeof(char) * 10) + 4 + PayloadLength;

            byte[] bytes = new byte[size];

            byte[] IdBytes = BitConverter.GetBytes(Id);
            byte[] TypeBytes = BitConverter.GetBytes((uint)Type);
            byte[] CallsignBytes = new byte[10];
            for (int i = 0; i < 10; ++i)
            {
                CallsignBytes[i] = (byte)Callsign[i];
            }

            byte[] PayloadLengthBytes = BitConverter.GetBytes(PayloadLength);


            Array.Copy(IdBytes, 0, bytes, 0, 4);
            Array.Copy(TypeBytes, 0, bytes, 4, 4);
            Array.Copy(CallsignBytes, 0, bytes, 8, 10);
            Array.Copy(PayloadLengthBytes, 0, bytes, 18, 4);

            if (PayloadLength > 0)
            {
                Array.Copy(Payload, 0, bytes, 22, PayloadLength);
            }

            return bytes;
        }

        public UInt32 Id;
        public PacketType Type;
        public char[] Callsign = new char[10];
        public int PayloadLength;
        public byte[] Payload;
    }

    internal class NetworkEndpoint
    {
        UdpClient client = new UdpClient(25565);
        Thread listenThread;

        Dictionary<UInt32, SentPacket> sentPackets = new Dictionary<UInt32, SentPacket>();
        List<UInt32> ackdPackets = new List<UInt32>();

        uint id = 0;
        string address = "192.168.128.12";


        public void Start()
        {
            listenThread = new Thread(new ThreadStart(listener));
            listenThread.Start();
        }

        public void listener()
        {
            while (true)
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                List<UInt32> updatePackets = new List<UInt32>();
                List<UInt32> removePackets = new List<UInt32>();

                foreach (SentPacket sp in sentPackets.Values)
                {
                    long timeDif = timestamp - sp.timestamp;

                    if(sp.retries > 5)
                    {
                        removePackets.Add(sp.p.Id);
                        Debug.WriteLine("Dropping packet " + sp.p.Id);
                        continue;
                    }

                    if (timeDif > 15)
                    {
                        Debug.WriteLine($"Packet {sp.p.Id} has not yet been ack'd, resending");
                        ResendPacket(sp.p);

                        // Add to list of packets that need timestamps to be updated
                        updatePackets.Add(sp.p.Id);
                    }
                }


                // Update timestamps and retries
                foreach (UInt32 i in updatePackets)
                {
                    SentPacket sp = sentPackets[i];
                    sp.timestamp = timestamp;
                    sp.retries += 1;
                    sentPackets[i] = sp;
                }

                foreach(UInt32 i in removePackets)
                {
                    sentPackets.Remove(i);
                }


                if (client.Available > 0)
                {
                    IPEndPoint ep = new IPEndPoint(IPAddress.Any, 0);
                    byte[] buf = client.Receive(ref ep);

                    Packet p = new Packet(buf);
                    if (p.Type != PacketType.BAD_PACKET)
                    {

                        Debug.WriteLine($"Received packet with ID {p.Id}");
                        switch (p.Type)
                        {
                            case PacketType.PACKET_ACK:
                                {
                                    
                                    uint ackId = p.Id;
                                    Debug.WriteLine($"Received ack for packet {ackId}");
                                    if (sentPackets.ContainsKey(ackId))
                                    {
                                        sentPackets.Remove(ackId);
                                        Debug.WriteLine("Packet has been acked");
                                    }
                                }
                                break;
                            case PacketType.GENERIC_PAYLOAD:
                                {
                                    Debug.WriteLine($"Received generic packet {p.Id}");
                                    if (!ackdPackets.Contains(p.Id))
                                    {
                                        byte[] textBuf = p.Payload;
                                        string rcvmsg = Encoding.ASCII.GetString(textBuf);
                                        Console.WriteLine(rcvmsg);
                                        Debug.WriteLine(rcvmsg);
                                    }

                                    uint id = p.Id;

                                    PacketAck(id);

                                }
                                break;
                            case PacketType.ECHO_REQUEST:
                                {
                                    Debug.WriteLine($"Received echo request {p.Id}");


                                    
                                    string echo = "ECHO: ";
                                    if (!ackdPackets.Contains(p.Id))
                                    {
                                        byte[] textBuf = p.Payload;
                                        string rcvmsg = Encoding.ASCII.GetString(textBuf);
                                        Debug.WriteLine(rcvmsg);
                                        Console.WriteLine(rcvmsg);
                                        echo += rcvmsg;
                                    }

                                    uint id = p.Id;

                                    PacketAck(id);

                                    Send(echo);

                                }
                                break;
                        }
                    }
                    else
                    {
                        Debug.WriteLine("Received a bad packet");
                    }

                    //string msg = Encoding.ASCII.GetString(buf);
                    //Console.WriteLine(msg);
                }

                Thread.Sleep(500);
            }
        }

        public Packet Send(string msg, bool echo = false)
        {
            byte[] buf = Encoding.UTF8.GetBytes(msg);

            Packet p = new Packet();
            p.SetCallsign("N7HUD");
            p.Id = id;
            p.Type = PacketType.GENERIC_PAYLOAD;
            
            if(echo)
            {
                p.Type = PacketType.ECHO_REQUEST;
            }
            
            p.Payload = buf;
            p.PayloadLength = buf.Length;

            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);

            SentPacket sp;
            sp.p = p;
            sp.timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            sp.retries = 0;

            Debug.WriteLine($"Sent packet id: {id}");
            sentPackets.Add(id++, sp);
            Debug.WriteLine("Adding sent packet the list");

            return p;
        }

        public void PacketAck(uint ackId)
        {

            Debug.WriteLine($"acking packet {ackId}");
            Packet p = new Packet();
            p.SetCallsign("N7HUD");
            p.Id = ackId;
            p.Type = PacketType.PACKET_ACK;
            p.PayloadLength = 0;

            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);

            if (!ackdPackets.Contains(ackId))
            {
                ackdPackets.Add(ackId);
            }
        }

        public void ResendPacket(Packet p)
        {
            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);
        }
    }
}
