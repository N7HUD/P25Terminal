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
        PACKET_NACK = 3021,
        RESEND_REQUEST = 3025,
        GENERIC_PAYLOAD = 4103,
        ECHO_REQUEST = 4104,
        INIT_FILE_TRANSFER = 4105,
        FILE_INFO = 4106,
        FILE_PART = 4107,
        FILE_SEND_COMPLETE = 4108,
        FILE_RECV_COMPLETE = 4109,
        FILE_PART_QUERY = 4110,
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

        public Packet(uint id, PacketType type, string callsign)
        {
            Id = id;
            Type = type;
            SetCallsign(callsign);
            PayloadLength = 0;
        }

        public Packet(uint id, PacketType type, string callsign, byte[] payload)
        {
            Id = id;
            Type = type;
            SetCallsign(callsign);
            PayloadLength = payload.Length;
            Payload = new byte[PayloadLength];
            Array.Copy(payload, Payload, PayloadLength);
        }




        // Packet transmit time is roughly 200 bytes per second. Retry time should be roughtly
        // (packet size/200) * 3
        // Empty packets should have a retry time of 15 seconds
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

        public long GetRetryTime()
        {
            if(PayloadLength > 0)
            {
                return (PayloadLength / 200) * 3;
            }
            else
            {
                return 15;
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
        public byte[]? Payload;
    }

    internal class NetworkEndpoint
    {
        UdpClient client = new UdpClient(25565);
        Thread? listenThread;

        Dictionary<UInt32, SentPacket> sentPackets = new Dictionary<UInt32, SentPacket>();
        List<UInt32> sentAcks = new List<UInt32>();
        List<UInt32> recvdAcks = new List<UInt32>();
        List<UInt32> recvdNacks = new List<UInt32>();


        bool fileReceiveInProgress = false;
        FileInfo receivedFileInfo = new FileInfo();
        Dictionary<uint, FilePart> receivedParts = new Dictionary<uint, FilePart>();

        uint id = 0;
        string address = "192.168.1.96";
        string callsign = "N7HUD";
        string downloadPath = "";

        public bool resend = true;

        Mutex packetMutex = new Mutex();

        bool DEBUG_DROP_PACKET = true;

        public NetworkEndpoint(string callsign, string address, string downloadPath)
        {
            this.callsign = callsign;
            this.address = address;
            this.downloadPath = downloadPath;
        }


        public void Start()
        {
            listenThread = new Thread(new ThreadStart(listener));
            listenThread.Start();
        }

        public bool IsPackedAcked(UInt32 id)
        {
            return recvdAcks.Contains(id);
        }

        public void listener()
        {
            while (true)
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                List<UInt32> updatePackets = new List<UInt32>();
                List<UInt32> removePackets = new List<UInt32>();

                if (resend)
                {
                    packetMutex.WaitOne();
                    foreach (SentPacket sp in sentPackets.Values)
                    {
                        long timeDif = timestamp - sp.timestamp;

                        if (sp.retries > 5)
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

                    foreach (UInt32 i in removePackets)
                    {
                        sentPackets.Remove(i);
                    }

                    packetMutex.ReleaseMutex();
                }


                while (client.Available > 0)
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
                                    packetMutex.WaitOne();
                                    uint ackId = p.Id;
                                    Debug.WriteLine($"Received ack for packet {ackId}");
                                    if (sentPackets.ContainsKey(ackId))
                                    {
                                        sentPackets.Remove(ackId);
                                        Debug.WriteLine("Packet has been acked");
                                    }
                                    packetMutex.ReleaseMutex();

                                    if(!recvdAcks.Contains(ackId))
                                    {
                                        recvdAcks.Add(ackId);
                                    }    
                                }
                                break;

                            case PacketType.PACKET_NACK:
                                {
                                    Debug.WriteLine($"Received nack for packet {p.Id}");
                                    if (!recvdNacks.Contains(p.Id))
                                    {
                                        recvdNacks.Add(p.Id);
                                    }
                                }
                                break;
                            case PacketType.GENERIC_PAYLOAD:
                                {
                                    Debug.WriteLine($"Received generic packet {p.Id}");
                                    if (!sentAcks.Contains(p.Id))
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
                                    if (!sentAcks.Contains(p.Id))
                                    {
                                        byte[] textBuf = p.Payload;
                                        string rcvmsg = Encoding.ASCII.GetString(textBuf);
                                        Debug.WriteLine(rcvmsg);
                                        Console.WriteLine(rcvmsg);
                                        echo += rcvmsg;
                                        Send(echo);
                                    }

                                    uint id = p.Id;

                                    PacketAck(id);

                                    

                                }
                                break;
                            case PacketType.INIT_FILE_TRANSFER:
                                {
                                    fileReceiveInProgress = true;
                                    Debug.WriteLine("Received file transfer request");
                                    Console.WriteLine("Remote is initiating a file transfer");
                                    PacketAck(p.Id);
                                }
                                break;
                            case PacketType.FILE_INFO:
                                {
                                    Debug.WriteLine("Received File info");
                                    if (p.Payload != null)
                                    {
                                        receivedFileInfo = FileInfo.CreateFromBytes(p.Payload);
                                        Debug.WriteLine($"Expecting to receive a file with {receivedFileInfo.fileParts} parts.");
                                        PacketAck(p.Id);
                                    }
                                    else
                                    {
                                        Debug.WriteLine("Received an invalid file info");
                                    }
                                }
                                break;
                            case PacketType.FILE_PART:
                                {
                                    Debug.WriteLine("Received file part");
                                    if(p.Payload != null)
                                    {
                                        FilePart fp = FilePart.CreateFromBytes(p.Payload);
                                        uint partId = fp.partId;

                                        if(partId == 3 && DEBUG_DROP_PACKET)
                                        {
                                            DEBUG_DROP_PACKET = false;
                                            break;
                                        }

                                        if(!receivedParts.ContainsKey(partId))
                                        {
                                            receivedParts.Add(partId, fp);
                                        }
                                        PacketAck(p.Id);
                                    }
                                }
                                break;
                            case PacketType.FILE_SEND_COMPLETE:
                                {
                                    if(receivedParts.Count == receivedFileInfo.fileParts)
                                    {
                                        Packet p1 = new Packet(id++, PacketType.FILE_RECV_COMPLETE, callsign);

                                        string filePath = downloadPath + "\\" + receivedFileInfo.fileName;

                                        FileStream fs;
                                        if (File.Exists(filePath))
                                        {
                                            fs = File.Open(filePath, FileMode.Truncate);
                                            Console.WriteLine("File already exists, overwriting");
                                        }
                                        else
                                        {
                                            fs = File.Create(filePath);
                                        }

                                        for (uint i = 0; i < receivedFileInfo.fileParts; ++i)
                                        {
                                            fs.Write(receivedParts[i].partData);
                                        }

                                        fs.Flush();
                                        fs.Close();

                                    }
                                    else
                                    {
                                        for(uint i = 0; i < receivedFileInfo.fileParts; ++i)
                                        {
                                            if(!receivedParts.ContainsKey(i))
                                            {
                                                Debug.WriteLine($"Missing file part {i}, asking to resend");
                                            }
                                        }
                                    }
                                }
                                break;
                            case PacketType.FILE_PART_QUERY:
                                {
                                    if(p.Payload != null)
                                    {
                                        Debug.WriteLine("Remote is requesting query of a file part");
                                        FilePart fp = FilePart.CreateFromBytes(p.Payload);

                                        uint queryId = fp.partId;
                                        Debug.WriteLine($"Remote asking about file part {queryId}");

                                        if(receivedParts.ContainsKey(queryId))
                                        {
                                            Debug.WriteLine("Sending ACK, we have the part");
                                            PacketAck(p.Id);
                                        }
                                        else
                                        {
                                            Debug.WriteLine("Sending NACK, we dont have that part");
                                            PacketNack(p.Id);
                                        }
                                    }
                                }
                                break;
                        }
                    }
                    else
                    {
                        Debug.WriteLine("Received a bad packet");
                    }
                }
            }
        }

        public Packet Send(byte[] buf, bool echo = false)
        {
            Packet p = new Packet();
            p.SetCallsign(callsign);
            p.Id = id;
            p.Type = PacketType.GENERIC_PAYLOAD;

            if (echo)
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


            // LOCK HERE
            packetMutex.WaitOne();
            Debug.WriteLine($"Sent packet id: {id}");
            sentPackets.Add(id++, sp);
            Debug.WriteLine("Adding sent packet the list");
            packetMutex.ReleaseMutex();

            return p;
        }

        public Packet Send(string msg, bool echo = false)
        {
            byte[] buf = Encoding.UTF8.GetBytes(msg);

            Packet p = new Packet();
            p.SetCallsign(callsign);
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

            // LOCK HERE
            packetMutex.WaitOne();
            Debug.WriteLine($"Sent packet id: {id}");
            sentPackets.Add(id++, sp);
            Debug.WriteLine("Adding sent packet the list");
            packetMutex.ReleaseMutex();

            return p;
        }

        public void PacketAck(uint ackId)
        {

            Debug.WriteLine($"acking packet {ackId}");
            Packet p = new Packet();
            p.SetCallsign(callsign);
            p.Id = ackId;
            p.Type = PacketType.PACKET_ACK;
            p.PayloadLength = 0;

            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);

            if (!sentAcks.Contains(ackId))
            {
                sentAcks.Add(ackId);
            }
        }

        public void PacketNack(uint ackId)
        {

            Debug.WriteLine($"Nacking packet {ackId}");
            Packet p = new Packet();
            p.SetCallsign(callsign);
            p.Id = ackId;
            p.Type = PacketType.PACKET_NACK;
            p.PayloadLength = 0;

            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);

            //if (!sentAcks.Contains(ackId))
            //{
            //    sentAcks.Add(ackId);
            //}
        }

        public void ResendPacket(Packet p)
        {
            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);
        }


        public void SendFile(NetworkFile file)
        {
            // Send init file transfer to request a connection
            Packet fileInit = new Packet(id++, PacketType.INIT_FILE_TRANSFER, callsign);
            byte[] fileInitBytes = fileInit.GetBytes();
            client.Send(fileInitBytes, fileInitBytes.Length, address, 25565);

            // Wait for ack
            while (!recvdAcks.Contains(fileInit.Id))
            {
                Thread.Sleep(100);
            }

            // Send file info
            FileInfo fi = file.GetInfo();
            Packet fileInfo = new Packet(id++, PacketType.FILE_INFO, callsign, fi.GetBytes());
            
            byte[] fileInfoBytes = fileInfo.GetBytes();
            client.Send(fileInfoBytes, fileInfoBytes.Length, address, 25565);

            // Wait for ack
            while (!recvdAcks.Contains(fileInfo.Id))
            {
                Thread.Sleep(100);
            }

            List<FilePart> fileParts = new List<FilePart>();
            for(int i = 0; i < fi.fileParts; ++i)
            {
                FilePart? fp = file.GetPart(i);

                if(fp != null)
                {
                    fileParts.Add(fp);
                }
            }

            List<uint> packetIds = new List<uint>();

            long listenTime = 0;
            long startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            long nowTime = startTime;
            // Send all file parts at once
            foreach (FilePart p in fileParts)
            {
                Packet partPacket = new Packet(id++, PacketType.FILE_PART, callsign, p.GetBytes());
                byte[] partPacketBytes = partPacket.GetBytes();
                
                packetIds.Add(partPacket.Id);
                listenTime += 10;

                client.Send(partPacketBytes, partPacketBytes.Length, address, 25565);
            }

            // Hopefully wait for all packets to be sent
            while((nowTime-startTime) < listenTime)
            {
                Thread.Sleep(100);
            }

            // Send File send complete
            Packet complete = new Packet(id++, PacketType.FILE_SEND_COMPLETE, callsign);
            byte[] completeBytes = complete.GetBytes();
            client.Send(completeBytes, completeBytes.Length, address, 25565);


            /*
            // Send all file parts
            for (int i = 0; i < fi.fileParts; ++i)
            {
                FilePart? fp = file.GetPart(i);
                if(fp != null)
                {
                    int retryCount = 0;

                    Packet partPacket = new Packet(id++, PacketType.FILE_PART, callsign, fp.GetBytes());
                    byte[] partPacketBytes = partPacket.GetBytes();

                    while (retryCount < 5)
                    {
                        long sentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        long timeNow = sentTime;

                        Debug.WriteLine($"Sending file part  {fp.partId}");
                        client.Send(partPacketBytes, partPacketBytes.Length, address, 25565);

                        Debug.WriteLine($"Waiting for ack for part {fp.partId}");
                        while (!recvdAcks.Contains(partPacket.Id) && (timeNow - sentTime) < partPacket.GetRetryTime())
                        {
                            timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            Thread.Sleep(100);
                        }

                        if(recvdAcks.Contains(partPacket.Id))
                        {
                            Debug.WriteLine($"Ack received for part {fp.partId}");
                            break;
                        }

                        Debug.WriteLine($"Missing ack for part {fp.partId}");

                        FilePart queryPart = new FilePart(fp.partId, new byte[1]);
                        Packet retryPacket = new Packet(id++, PacketType.FILE_PART_QUERY, callsign, queryPart.GetBytes());


                        Debug.WriteLine($"Sending part query for part {fp.partId}");
                        client.Send(retryPacket.GetBytes(), retryPacket.GetBytes().Length, address, 25565);

                        sentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        while (!recvdAcks.Contains(retryPacket.Id) && !recvdNacks.Contains(retryPacket.Id) && (timeNow - sentTime) < partPacket.GetRetryTime())
                        {
                            timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                            Thread.Sleep(100);
                        }

                        

                        if(recvdAcks.Contains(retryPacket.Id))
                        {
                            // An ack means client has the part
                            Debug.WriteLine($"Ack received for query on file part {fp.partId}, client has part, moving on");
                            break;
                        }

                        ++retryCount;

                        // This is not strictly necessary
                        if (recvdNacks.Contains(retryPacket.Id))
                        {
                            // An ack means client has the part
                            Debug.WriteLine($"Nack received for query on file part {fp.partId}, retrying");
                            
                            continue;
                        }

                        Debug.WriteLine($"Missing nack and ack for query on file part {fp.partId}, retrying");

                    }

                    if(retryCount >= 5)
                    {
                        Debug.WriteLine("We seem to have lost connection, aborting");
                        return;
                    }
                }
            }
            */

            //// Send File send complete
            //Packet complete = new Packet(id++, PacketType.FILE_SEND_COMPLETE, callsign);
            //byte[] completeBytes = complete.GetBytes();
            //client.Send(completeBytes, completeBytes.Length, address, 25565);

            // Wait for file recv complete or empty file part packets

            // Receiving a file part after init sending indicates the
            // receiver wants that file part again
        }

        public Packet SendFileInfo(FileInfo fileInfo)
        {
            Packet p = new Packet();
            p.SetCallsign(callsign);
            p.Id = id;
            p.Type = PacketType.FILE_INFO;

            p.Payload = fileInfo.GetBytes();
            p.PayloadLength = p.Payload.Length;

            byte[] packetBytes = p.GetBytes();

            client.Send(packetBytes, packetBytes.Length, address, 25565);

            SentPacket sp;
            sp.p = p;
            sp.timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            sp.retries = 0;


            // LOCK HERE
            packetMutex.WaitOne();
            Debug.WriteLine($"Sent packet id: {id}");
            sentPackets.Add(id++, sp);
            Debug.WriteLine("Adding sent packet the list");
            packetMutex.ReleaseMutex();

            return p;
        }
    }
}
