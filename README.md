# P25 Terminal
The P25 Terminal project is a lightweight network framework to support IP over P25 connections for the transfer of arbitrator data packets using P25 radios. Most P25 radios are capable of supporting IP packet data which make them a great option for keybaord to keybaord data as well as small file payloads. This project implements a simple UDP data protocol that supports basic traffic acknowledgement and resending without all the TCP overhead. This protocol is designed to support amateur radio operation by including a callsign segment of every packet sent. For FCC Part 90 licenses that support data emissions, the callsign segment can be used to identify specific stations or can be completely ignored. 

## Packet Layout
Every packet follows the same basic layout with certain payload types following a sub packet layout. The basic packet layout is as follows:

* UInt32 Packet ID
* UInt32 Packet Type (typed enum)
* char[10] Callsign
* Int32 Payload Size
* byte[] Payload

Packet ID: The packet ID identifies the packet number for tracking both packet order as well as packet acknowledgement and resending. Each client maintains their own packet ID counter that corresponds to packets they have sent. 

Packet Type: The packet type specifies if a sub packet payload is present and if so which type. Some packet types such as PACKET_ACK and PACKET_NACK do not require the payload to be populated.

Callsign: The callsign segment is present in every packet to satisfy FCC requirements for station identification when operating with a valid amateur radio license. For part 90 licensed stations it can be populated with part 90 callsign or with some other station identifier.

Payload Size: The payload size specifies how many bytes are present in the payload. If payload size is zero then the payload is unspecified and should be ignored.

Payload: A variable size array containing the packet payload. This may either be an unspecified payload type (usually just a text string in this case) or it may be a sub-packet as specified by the Packet Type.

## File Transfer Sub-Packet Layouts
These packets and sub-packets are used to facilitate the transfer of a file from sender to receiver. 

### File Info
The file info packet is used when sending a file from one client to another. File transfer begins with an empty packet of type INIT_FILE_TRANSFER. After the INIT_FILE_TRANSFER packet is acknowledged, the sender begins the file transfer with a FILE_INFO packet that contains a FileInfo sub-packet. The file info packet contains information necessary to handle the file transfer as well as information about the file such as its name and its size. The FileInfo sub-packet layout is as follows:

* Int32 File Name Size
* char[] File Name
* Int64 File Size
* Int32 File Parts Count
* Int32 Chunk Size
* Int32 Chunk Wait Time

File Name Size: The size in bytes of the file name.

File Name: A char array of length specified in the File Name Size that contains the name of the file not including any path information. Internally this is stored as a string and converted to a char array during transmit.

File Size: The number of bytes in the file.

File Parts Count: The number of file segments that will be transmitted. Each file is broken up into segments called file parts that contain up to a fixed number of bytes. Currently that size is 2048 bytes and can be modified in the FileGlobals static class. The 2048 file part size strikes a balance between preventing packet fragmentation and limiting overheard per byte sent. A smaller size may provide more robust packet coherency (limited fragmentation) but may risk longer transfer times due to more packets sent as well as potential for more dropped packets needing to be resent.

Chunk Size: The number of file parts to send at a time. Currently this is hard coded to 5, meaning 5 file parts will be sent at a time with a designated wait time between each chunk to allow the UDP transmit buffers to drain. Since P25 data transmission is much slower than typical transmission mediums it can take many seconds for a packet to complete transmission.

Chunk Wait Time: The time in seconds to wait between sending file part chunks. This value can be user specified in a config file but defaults to 35 seconds. This time is heuristically set to allow 5 FilePart packets to be sent to completion before transmitting any more packets and potentially causing packets to be dropped by the sender. Increasing this time may make file transfer more robust at the expense of more dead air time during transfer. It is not recommended to lower this time as lower it may cause too many packets to be sent at once which will increase the number of packets that need to be retransmitted. If the file part size or chunk size are modified this number will need to be modified. For a file part size of 2048 the time to wait per file part is exactly 7 seconds (35/5).

### File Part
A file part packet has a type of FILE_PART and contains a small piece of the file being transmitted. The FilePart sub-packet layout is as follows:

* UInt32 Part ID
* UInt16 Part Size
* byte[] Part Data
* byte[16] Part Data Hash

Part ID: The Part ID defines the order of the parts being transmitted. During reconstruction of the file the part IDs will be walked in order and written to the file in order. The part ID is also used by the receiver to request retransmission of any parts that were not received during the initial transfer. 

Part Size: The size of the part in bytes. A 16 bit int was chosen to save space in the packet as the maximum size of a 16 bit int is far larger than the most common MTU value for jumbo frames. 

Part Data: A byte array of size Part Size that holds the specified part data for the given part of the file being transferred. This array should never be empty.

Part Data Hash: This array is currently an empty 16 byte array, but is reserved to hold a 128 bit MD5 hash of the part data being sent. When populated this byte array should only ever contain the MD5 hash of the data present in the Part Data array. FCC rules for amateur radio operation prohibit operators from encrypting or obscuring any message sent over amateur frequencies, this section must always either be empty or contain the MD5 hash of the Part Data array such that the same hash should be obtained when calculating the MD5 hash of the Part Data. If the MD5 hash present in a received packet does not match the MD5 hash calculated on the Part Data then the packet was corrupted in flight and should be discarded, generating a resend request. UDP does not provide any correctness data for user payloads unlike TCP and this field allows for basic error checking. So far in testing this field has not been necessary.

### File Resend Request
A File Resend Request is used when a sender has completed sending a file but the receiver has not received all parts of the file. The File Resend Request sub-packet layout is as follows:

* UInt32 Resend Count
* UInt32[] Part ID List

Resend Count: The number of file parts the receiver is requesting to be resent.

Part ID List: A list of file part IDs the receiver is requesting to be resent. The sender will resend all file parts listed by ID in this list. Currently the sender sends all of these parts at once which may result in multiple resend requests if any resent parts are dropped. In the future the sender should batch the resent packets in the same way that the initial parts are batched.

### File Part Query
A File Part Query is a sub-packet of type FILE_PART_QUERY and is no longer necessary for the transfer of files. Initially the resending of file parts was implemented as a push model where the sender would ask which parts the receiver has and resend any missing parts. Through testing it was found that a pull model works better, allowing the receiver to request parts that are missing. As the packet type is still present in code and properly handled it is present in the documentation. The File Part Query sub-packet consists of a FilePart sub-packet that contains a valid part ID, but a Part Size of 0 and an empty Part Data and Part Data Hash array. The part ID is then used to query the list of received parts and either an ACK is returned if the part is present, or a NACK if it is not present.

### File Send Complete
A File Send Complete packet is sent when the sender has completed sending all parts of the file. This packet signals the receiver that all parts have completed sending and that any missing parts should be re-request. This packet has no payload data or sub-packet and has a packet type of FILE_SEND_COMPLETE.

### File Receive Complete
A File Receive Complete packet is sent when the receiver has successfully received all parts of the file. This packet signals the sender that it no longer needs to respond to file resend requests and has a packet type of FILE_RECV_COMPLETE. The receipt of this packet signals the sender that it can clear any memory associated with the sent file and that it is safe to send a consecutive file if needed by the user. Currently there are no safeguards in the code to prevent the user from initiating a subsequent file transfer before the previous is complete but this safe guard should be implemented. 

## Generic Packet Types
The following packet types and layouts can be used generically for multiple uses or for flexible use cases depending on source modifications.

### Packet ACK
A Packet ACK is a simple ACK packet that contains an empty payload, but has a packet ID that is equal to the received packet that is being ACK'd. This represents the acknowledgement to the sender that the receiver has successfully received the packet. This packet should be sent after the receipt of every packet that is expected to be reliable. This packet has the type PACKET_ACK.

### Packet NACK
A Packet NACK is the opposite of the ACK packet and also contains an empty payload. The NACK packet tells the sender that the receiver did not correctly receive a packet or that the receiver does not have some data that the sender is asking about. Currently this packet is only sent in response to a File Part Query. This packet has the type PACKET_NACK.

### Generic Payload
A Generic Payload packet contains an unspecified data payload and has a type of GENERIC_PAYLOAD. It is up to the client to determine how to interpret the data. Currently this packet is used to send plain text between two clients to facilitate keyboard to keyboard text messaging. For use on amateur radio frequencies this packet type should only ever contain plain text data such that anyone receiving these packets can determine their meaning and intent as required by FCC rules.

### Echo Request
An Echo Request is similar to the current implementation of the Generic Payload except it specifies that the receiver should "echo" back the plain text sent in the payload. An Echo Request packet has the type of ECHO_REQUEST. Currently as implemented the receiver resends the payload to the sender with the string "ECHO: " prepended to the payload. 

## Other Packet Types
Below is a list of other packet types that are either place holders or not currently implemented.

### Bad Packet
A packet that is received malformed will result in a Packet object of type BAD_PACKET. These packets get dropped by the receiver.

### Client Connect
Currently not implemented. This packet type is meant to facilitate the intelligent connection of two clients. The packet type for Client Connect packets is CLIENT_CONNECT.

### Client Connect ACK
Currently not implemented. This packet type is meant to acknowledge that a client is requesting to connect. It would be sent as a direct response to a Client Connect packet and has a packet type of CLIENT_CONNECT_ACK.

### Client Disconnect
Currently not implemented. This packet type is meant to signal that a client is disconnecting from another client. No ACK would be required for this packet. The packet type for Client Disconnect packets is CLIENT_DISCONNECT.