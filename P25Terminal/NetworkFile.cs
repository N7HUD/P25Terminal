using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Diagnostics;

namespace P25Terminal
{
    public static class FileGlobals
    {
        public const int MAX_PART_SIZE = 2048;
    }

    public class FileInfo
    {
        public int fileNameSize = 0;
        public string fileName = "";
        public long fileSize = 0;
        public int fileParts = 0;

        public byte[] GetBytes()
        {
            byte[] nameBytes = Encoding.ASCII.GetBytes(fileName);
            fileNameSize = nameBytes.Length;

            byte[] data = new byte[4 + fileNameSize + 8 + 4];
            Array.Copy(BitConverter.GetBytes(fileNameSize), 0, data, 0, 4);
            Array.Copy(nameBytes, 0, data, 4, fileNameSize);
            Array.Copy(BitConverter.GetBytes(fileSize), 0, data, 4 + fileNameSize, 8);
            Array.Copy(BitConverter.GetBytes(fileParts), 0, data, 4 + 8 + fileNameSize, 4);


            return data;
        }

        public static FileInfo CreateFromBytes(byte[] bytes)
        {
            FileInfo fi = new FileInfo();
            fi.fileNameSize = BitConverter.ToInt32(bytes, 0);
            fi.fileName = Encoding.ASCII.GetString(bytes, 4, fi.fileNameSize);
            fi.fileSize = BitConverter.ToInt64(bytes, fi.fileNameSize + 4);
            fi.fileParts = BitConverter.ToInt32(bytes, fi.fileNameSize + 4 + 8);

            return fi;
        }
    }

    public class FilePart
    {
        public UInt32 partId;
        public UInt16 partSize;
        public byte[] partData;
        public byte[] partHash = new byte[16];
        MD5 md5 = MD5.Create();

        private FilePart() { partData = new byte[1]; }

        public FilePart(UInt32 id, byte[] data)
        {
            if (data.Length > FileGlobals.MAX_PART_SIZE)
            {
                throw new Exception("A file part size cannot exceed 2048 bytes");
            }

            partId = id;
            partSize = (UInt16)data.Length;
            partData = new byte[partSize];
            Array.Copy(data, partData, partSize);

            byte[] hash = md5.ComputeHash(partData);
            Array.Copy(hash, partHash, hash.Length);
        }

        public byte[] GetBytes()
        {
            byte[] data = new byte[4 + 2 + partData.Length + 16];
            Array.Copy(BitConverter.GetBytes(partId), 0, data, 0, 4);
            Array.Copy(BitConverter.GetBytes(partSize), 0, data, 4, 2);
            Array.Copy(partData, 0, data, 6, partSize);
            Array.Copy(partHash, 0, data, 6 + partSize, 16);

            return data;
        }

        public static FilePart CreateFromBytes(byte[] packetBytes)
        {
            FilePart fp = new FilePart();

            fp.partId = BitConverter.ToUInt32(packetBytes, 0);
            fp.partSize = BitConverter.ToUInt16(packetBytes, 4);
            fp.partData = new byte[fp.partSize];
            Array.Copy(packetBytes, 6, fp.partData, 0, fp.partSize);
            Array.Copy(packetBytes, 6 + fp.partSize, fp.partHash, 0, 16);

            return fp;
        }
    }


    internal class NetworkFile
    {
        FileInfo info = new FileInfo();
        List<FilePart> parts = new List<FilePart>();

        public NetworkFile(string filePath)
        {
            FileStream fs = File.OpenRead(filePath);
            long size = fs.Length;

            byte[] fileData = new byte[size];

            fs.Read(fileData, 0, (int)size);

            info.fileName = Path.GetFileName(filePath);
            info.fileSize = size;

            int wholeParts = (int)size / FileGlobals.MAX_PART_SIZE;
            int remainingBytes = (int)size % FileGlobals.MAX_PART_SIZE;
            int halfParts = 0;

            if (remainingBytes != 0)
            {
                halfParts = 1;
            }

            info.fileParts = wholeParts + halfParts;

            for (int i = 0; i < wholeParts; ++i)
            {
                byte[] tmpbuf = new byte[FileGlobals.MAX_PART_SIZE];
                Array.Copy(fileData, i * FileGlobals.MAX_PART_SIZE, tmpbuf, 0, FileGlobals.MAX_PART_SIZE);
                FilePart fp = new FilePart((UInt32)i, tmpbuf);
                parts.Add(fp);
            }

            if (halfParts > 0)
            {
                int recordedBytes = (wholeParts * FileGlobals.MAX_PART_SIZE);
                long dif = size - recordedBytes;
                byte[] tmpbuf = new byte[dif];

                Array.Copy(fileData, recordedBytes, tmpbuf, 0, dif);
                FilePart fp = new FilePart((UInt32)wholeParts, tmpbuf);
                parts.Add(fp);
            }

            Debug.WriteLine($"Recorded {parts.Count} File parts");

        }

        public FileInfo GetInfo()
        {
            return info;
        }

        public FilePart? GetPart(int partId)
        {
            if(partId < 0 || partId > parts.Count)
            {
                return null;
            }
            else
            {
                return parts[partId];
            }
        }
    }
}
