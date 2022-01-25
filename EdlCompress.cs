using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;

// ReSharper disable SuggestBaseTypeForParameterInConstructor

namespace wdcossey
{
    public interface IEdlCompress
    {
        Stream Decompress(string fileName);
        Stream Decompress(Stream @in);

        public EdlCompress.EdlHeader ParseHeader(string fileName);
        public EdlCompress.EdlHeader ParseHeader(Stream @in);

        IEnumerable<EdlCompress.EdlHeader> Scan(string fileName);
        IEnumerable<EdlCompress.EdlHeader> Scan(Stream @in);
    }

    public class EdlCompress : IEdlCompress
    {
        public enum EdlEndianType : int
        {
            /// <summary>
            /// Little (0) Endian
            /// </summary>
            Little = 0,

            /// <summary>
            /// Big (1) Endian
            /// </summary>
            Big = 1
        }

        public record EdlHeader
        {
            private static readonly char[] EdlHeaderIdentifier = { 'E', 'D', 'L' };

            /// <summary>compression type 0-2</summary>
            public int CompressionType { get; set; }

            /// <summary>big(1) or little(0) endian</summary>
            public EdlEndianType Endian { get; set; }

            /// <summary>compressed size</summary>
            public long CompressedSize { get; set; }

            /// <summary>decompressed size</summary>
            public long DecompressedSize { get; set; }

            public static EdlHeader Parse(BinaryReader reader)
            {
                var headerChars = reader.ReadChars(3);

                if (!headerChars.SequenceEqual(EdlHeaderIdentifier))
                    throw new InvalidOperationException("Does not contain a valid EDL header");

                var compressionType = reader.ReadByte();
                var endianType = (EdlEndianType)(compressionType >> 7);

                var compressedSize = reader.ReadUInt32();
                var decompressedSize = reader.ReadUInt32();

                if (endianType == EdlEndianType.Big)
                {
                    compressedSize = ByteSwap(compressedSize);
                    decompressedSize = ByteSwap(decompressedSize);
                }

                return new EdlHeader
                {
                    Endian = endianType,
                    CompressionType = compressionType & 0xF,
                    CompressedSize = Convert.ToInt64(compressedSize),
                    DecompressedSize = Convert.ToInt64(decompressedSize),
                };
            }
        }

        public record EdlOffsetHeader : EdlHeader
        {
            public long Offset { get; set; }

            public new static EdlHeader Parse(BinaryReader reader)
            {
                var offset = reader.BaseStream.Position - 3;
                var x = reader.ReadByte();
                var compressionType = x & 0xF;
                var endianType = (EdlEndianType)(x >> 7);

                var compressedSize = reader.ReadUInt32();
                var decompressedSize = reader.ReadUInt32();

                if (endianType == EdlEndianType.Big)
                {
                    compressedSize = ByteSwap(compressedSize);
                    decompressedSize = ByteSwap(decompressedSize);
                }

                return new EdlOffsetHeader
                {
                    Endian = endianType,
                    CompressionType = compressionType,
                    CompressedSize = Convert.ToInt64(compressedSize),
                    DecompressedSize = Convert.ToInt64(decompressedSize),
                    Offset = offset
                };
            }
        }

        private readonly ILogger _logger;

        public EdlCompress() { }

        public EdlCompress(ILogger<EdlCompress> logger)
            : this() => _logger = logger;

        private static uint ByteSwap(ulong w)
        {
            return (uint)((w >> 24) | ((w >> 8) & 0x0000ff00) | ((w << 8) & 0x00ff0000) | (w << 24));
        }

        private int erratta(long code)
        {
            switch (code)
            {
                case -8:
                    _logger?.LogError("Not a valid table entry");
                    return (int)code;
                case -9:
                    _logger?.LogError("Samples exceed maximum bitcount");
                    return (int)code;
                case -12:
                    return (int)code;
                default:
                    _logger?.LogError("Unknown error {Code}", code);
                    return 0;
            }
        }

        private static long Helper(ref ulong data, long bitCount, BinaryReader @in, long streamOffset, ref long pos, EdlHeader header)
            => Helper(ref data, bitCount, @in, streamOffset, ref pos, header.CompressedSize, header.Endian);

        private static long Helper(ref ulong data, long bitCount, BinaryReader @in, long streamOffset, ref long pos, long max, EdlEndianType endian)
        {
            if (bitCount > 32)
                return bitCount; //essentially, do nothing!

            var z = (uint)data;
            var x = max - pos;
            if (x > 4)
                x = 4; //#bytes to retrieve from file

            // NOTE: If we are at the end of the data, and we are trying to get more data
            //		 don't do so and just return
            if (@in.BaseStream.Position == @in.BaseStream.Length)
                return x;

            @in.BaseStream.Seek(streamOffset + pos, SeekOrigin.Begin);
            var y = @in.ReadUInt32();

            if (endian != EdlEndianType.Little)
            {
                //y = y.Reverse().ToArray();
                y = ByteSwap(y);
            }

            pos += x;

            data = y; //tack old data on the end of new data for a continuous bitstream
            data <<= (int)bitCount;
            data |= z;

            x *= 8; //revise bitCount with number of bits retrieved
            return bitCount + x;
        }

        /// <summary>
        /// Generate Tables
        /// </summary>
        /// <param name="large"></param>
        /// <param name="what"></param>
        /// <param name="total"></param>
        /// <param name="num"></param>
        /// <param name="bufSize"></param>
        /// <returns></returns>
        private int FillBuffer(ref ushort[] large, ref byte[] what, int total, long num, int bufSize)
        {
            byte[] buf = new byte[1 << bufSize];
            ushort[] when = new ushort[num];
            ushort[] samp = new ushort[num];
            uint[] number = new uint[16];
            int x;
            int y;
            int z;
            int back;

            try
            {
                /*my implementation is stupid and always copies the block, so this uses even more memory than it should
				if(!(what=realloc(what,num))
				    {printf("\nVirtual memory exhausted.\nCan not continue.\n\tPress ENTER to quit.");
				    getchar();
				    return 0;
				    }*/

                //memset(large, 0, 0xC00); //both buffers have 0x600 entries each
                Array.Resize(ref large, 0xC00);
                Array.Fill(large, (byte)0, 0, 0xC00);

                /*build an occurance table*/
                back = 0; //back will act as a counter here
                for (y = 1; y < 16; y++) //sort occurance
                {
                    for (x = 0; x < total; x++) //peek at list
                    {
                        if (what[x] == y)
                        {
                            when[back] = (ushort)x;
                            back++;
                            number[y]++;
                        }
                    } //end peek
                } //end occurrence

                x = 0;
                for (y = 1; y < 16; y++) //sort nibbles
                {
                    for (z = (int)number[y]; z > 0; z--)
                    {
                        what[x] = (byte)y;
                        x++;
                    }
                } //end sort

                Array.Resize(ref number, 0);

                /*generate bitsample table*/
                z = what[0]; //first sample, so counting goes right
                back = 0; //back will act as the increment counter
                for (x = 0; x < num; x++)
                {
                    y = what[x];
                    if (y != z)
                    {
                        z = y - z;
                        back *= (1 << z);
                        z = y;
                    }

                    y = (1 << y) | back;
                    back++;
                    do
                    {
                        samp[x] = (ushort)(samp[x] << 1);
                        samp[x] += (ushort)(y & 1);
                        y = y >> 1;
                    } while (y != 1);
                } //end bitsample table

                for (x = 0; x < num; x++) //fill buffer    8001392C
                {
                    back = what[x]; //#bits in sample
                    if (back < bufSize) //normal entries
                    {
                        y = 1 << back;
                        z = samp[x]; //offset within buffer
                        do
                        {
                            large[z] = (ushort)((when[x] << 7) + what[x]);
                            z += y;
                        } while ((z >> bufSize) == 0);
                    } //end normal
                    else
                    {
                        y = (1 << bufSize) - 1; //this corrects bitmask for buffer entries
                        z = samp[x] & y;
                        buf[z] = what[x];
                    } //end copies
                } //end fill

                /*read coded types > bufsize    80013AA8*/
                z = 0; //value
                       //for (x = 0; !(x >> bufsize); x++)/*read buf*/
                for (x = 0; (x >> bufSize) == 0; x++) //read buf
                {
                    y = buf[x];
                    if (y != 0)
                    {
                        y -= bufSize;
                        if (y > 8)
                        {
                            Array.Resize(ref buf, 0);
                            return -8;
                        }

                        back = (z << 7) + (y << 4); //value*0x80 + bits<<4
                        large[x] = (ushort)back;
                        z += (1 << y);
                    } //end if(y)
                } //end buf reading


                Array.Resize(ref buf, 0);

                if (z > 0x1FF)
                    return -9;

                /*do something tricky with the special entries    80013B3C*/
                back = 1 << bufSize;
                for (x = 0; x < num; x++)
                {
                    if (what[x] < bufSize)
                        continue;

                    z = samp[x] & (back - 1);
                    z = large[z]; //in dASM, this is labelled 'short'
                    y = samp[x] >> bufSize;
                    /*80013BEC*/
                    do
                    {
                        //large[y + (z >> 7) + (1 << bufsize)] = (ushort)((when[x] << 7) + what[x]);
                        var index = y + (z >> 7) + (1 << bufSize);
                        large[index] = (ushort)((when[x] << 7) + what[x]);
                        y = y + (1 << (what[x] - bufSize));
                    } while ((y >> ((z >> 4) & 7)) == 0);
                }

                return 0;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{Message}", ex.Message);
                return -12;
            }
            finally
            {
                Array.Resize(ref when, 0);
                Array.Resize(ref samp, 0);
                Array.Resize(ref buf, 0);
                Array.Resize(ref number, 0);
            }
        }

        public Stream Decompress(string fileName)
        {
            using var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
            return Decompress(fileStream);
        }

        public Stream Decompress(Stream @in)
        {
            var buffer = DecompressAsSpan(@in);
            var result = new MemoryStream();
            result.Write(buffer);
            result.Seek(0, SeekOrigin.Begin);
            return result;
        }

        public EdlHeader ParseHeader(string fileName)
        {
            using var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
            return ParseHeader(fileStream);
        }

        public EdlHeader ParseHeader(Stream @in)
            => GetEdlHeader(@in);

        public IEnumerable<EdlHeader> Scan(string fileName)
        {
            using var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
            return Scan(fileStream);
        }

        /// <summary>
        /// Gets the offsets of the EDL
        /// </summary>
        /// <param name="in"></param>
        /// <returns></returns>
        public IEnumerable<EdlHeader> Scan(Stream @in)
        {
            var streamOffset = @in.Position;

            var result = new List<EdlHeader>();

            using var binaryReader = new BinaryReader(input: @in, encoding: Encoding.ASCII, leaveOpen: true);

            while (@in.Position < (@in.Length - 3))
            {
                if (binaryReader.ReadChar() != 'E')
                    continue;

                if (binaryReader.ReadChar() != 'D')
                    continue;

                if (binaryReader.ReadChar() != 'L')
                    continue;

                result.Add(EdlOffsetHeader.Parse(binaryReader));
            }

            @in.Seek(streamOffset, SeekOrigin.Begin);

            //_logger?.LogTrace("{Offsets}", string.Join(",", result.Select(s => $"0x{s:x8}")));
            return result.ToArray();
        }

        private Span<byte> DecompressAsSpan(Stream @in)
        {
            var streamOffset = @in.Position;

            using var binaryReader = new BinaryReader(input: @in, encoding: Encoding.ASCII, leaveOpen: true);

            var header = EdlHeader.Parse(binaryReader);

            return header.CompressionType switch
            {
                0 => DecompressEdl0(binaryReader, header, streamOffset),
                1 => DecompressEdl1(binaryReader, header, streamOffset),
                _ => throw new InvalidOperationException($"Unsupported compression type ({header.CompressionType})")
            };
        }

        private EdlHeader GetEdlHeader(Stream @in)
        {
            var streamOffset = @in.Position;

            using var binaryReader = new BinaryReader(input: @in, encoding: Encoding.ASCII, leaveOpen: true);

            var header = GetEdlHeader(binaryReader);

            @in.Seek(streamOffset, SeekOrigin.Begin);

            return header;
        }

        private EdlHeader GetEdlHeader(BinaryReader binaryReader)
        {
            var header = EdlHeader.Parse(binaryReader);
            return header;
        }

        /*These are the three known decompression routines as stripped from TWINE (N64)
		 as a note, the endian value is not used,
		 since I can't confirm that it is used outside of the header size values
		 Each returns the size of the decompressed file, which can be tested against expected*/

        #region EDL Type 0 Decompression

        /// <summary>
        /// Store Decompression
        /// </summary>
        /// <param name="in"></param>
        /// <param name="header"></param>
        /// <param name="streamOffset"></param>
        /// <returns></returns>
        private static Span<byte> DecompressEdl0(BinaryReader @in, EdlHeader header, long streamOffset)
        {
            var fileSize = @in.BaseStream.Length - 12;
            var length = Math.Min(fileSize, header.DecompressedSize);
            Span<byte> result = new byte[length];
            @in.BaseStream.Seek(streamOffset + 12, SeekOrigin.Begin);
            @in.Read(result);
            return result;
        }

        #endregion

        #region EDL Type 1 Decompression

        /// <summary>
        /// Cool `bitwise table type` Decompression
        /// </summary>
        /// <param name="in"></param>
        /// <param name="header"></param>
        /// <param name="streamOffset"></param>
        /// <returns></returns>
        private Span<byte> DecompressEdl1(BinaryReader @in, EdlHeader header, long streamOffset)
        {
            var bits = new byte[9]; //what=p->list of slots
            long x;
            long y;
            long z;
            int stack = 0;
            long count = 0;
            long num;
            uint back; //count=#bits in register, num=#to copy, back=#to backtrack
            ushort[] smallArray = new ushort[0x600];
            ushort[] large = new ushort[0x600]; //when=p->occurance in list

            var arrayIndex = 0;

            var resultBuffer = new byte[header.DecompressedSize];
            Span<byte> result = resultBuffer;

            #region Tables

            byte[] table1 =
            {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 0xA, 0xC, 0xE, 0x10, 0x14, 0x18, 0x1C, 0x20, 0x28, 0x30, 0x38, 0x40, 0x50,
                0x60, 0x70, 0x80, 0xA0, 0xC0, 0xE0, 0xFF, 0, 0, 0
            };

            byte[] table2 =
            {
                0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0, 0, 0, 0
            };

            ushort[] table3 =
            {
                0, 1, 2, 3, 4, 6, 8, 0xC, 0x10, 0x18, 0x20, 0x30, 0x40, 0x60, 0x80, 0xC0, 0x100, 0x180, 0x200, 0x300,
                0x400, 0x600, 0x800, 0xC00, 0x1000, 0x1800, 0x2000, 0x3000, 0x4000, 0x6000
            };

            byte[] table4 =
            {
                0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 0xA, 0xA, 0xB, 0xB, 0xC, 0xC, 0xD, 0xD, 0, 0
            };

            #endregion

            var what = new byte[0x400];
            ulong data = 0; //64bit datatable container
            long pos = 0;

            try
            {

                for (pos += 12; pos <= header.CompressedSize; back = 0)
                {
                    //memset(bits, 0, 8); //clear bits between... stuff
                    Array.Fill(bits, (byte)0, 0, 8);

                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                    x = (int)(data & 1);
                    data >>= 1;
                    count--;

                    if (x != 0) //mode 1 - tables
                    {
                        count = Helper(ref data, count, @in, streamOffset, ref pos, header); //build large table
                        x = (int)(data & 0x1FF);
                        data >>= 9;
                        count -= 9;

                        _logger?.LogTrace("mode1\tpos: {Position}\tout: {Index}\tdata: 0x{Data:x8}", pos, arrayIndex,
                            data);

                        if (x != 0) //construct tables
                        {
                            Array.Fill(what, (byte)0, 0, 0x400);

                            num = 0; //true # entries, since 0 entries are not counted!
                            for (y = 0; y < x; y++) //fill table with nibbles
                            {
                                count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                back = (uint)(data & 1);
                                data >>= 1;
                                count--;
                                if (back != 0) //grab nibble
                                {
                                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                    stack = (int)(data & 0xF);
                                    data >>= 4;
                                    count -= 4;
                                } //end grab

                                what[y] = (byte)stack;
                                if (stack != 0)
                                    num++; //count nonzero entries

                            } //end fill

                            x = FillBuffer(ref large, ref what, (int)x, num, 10);

                        } //end construction

                        if (x < 0)
                        {
                            x = erratta(x);
                            if (x != 0)
                                return result[..];
                        }

                        count = Helper(ref data, count, @in, streamOffset, ref pos, header); //build smaller table
                        x = (long)(data & 0x1FF);
                        data >>= 9;
                        count -= 9;
                        if (x != 0) //construct tables
                        {
                            Array.Fill(what, (byte)0, 0, 0x400);

                            num = 0; //true # entries, since 0 entries are not counted!
                            for (y = 0; y < x; y++) //fill table with nibbles
                            {
                                count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                back = (uint)(data & 1);
                                data >>= 1;
                                count--;
                                if (back != 0) //grab nibble
                                {
                                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                    stack = (int)(data & 0xF);
                                    data >>= 4;
                                    count -= 4;
                                } //end grab

                                what[y] = (byte)stack;
                                if (stack != 0)
                                    num++; //count nonzero entries

                            } //end fill

                            x = FillBuffer(ref smallArray, ref what, (int)x, num, 8);

                        } //end construction

                        if (x < 0)
                        {
                            x = erratta(x);
                            if (x != 0)
                                return result[..];
                        }

                        /*write data*/
                        do
                        {
                            count = Helper(ref data, count, @in, streamOffset, ref pos, header); //build smaller table

                            x = (long)data & 0x3FF;
                            x = large[x]; //x=short from thingy
                            y = x & 0xF; //y=normal bitcount
                            z = ((int)x >> 4) & 7; //z=backtrack bitcount

                            _logger?.LogTrace("out: {Pos}\tsample: 0x{Sample:x4}\tvalue: {Value}\tdata: 0x{Data:x8}",
                                arrayIndex, x, x >> 7, data);

                            //Console.WriteLine("count: {0:X}; pos: {1:X}; data: {2:X}; x: {3:X}; y: {4:X}; z: {5:X};", count, pos, data, x, y, z);
                            //if (flagrant.message)
                            {
                                //Console.WriteLine("\tout: {0:X}\tsample: {1:X4}\tvalue: {2:X}\tdata: 0x{3:X}", @out.BaseStream.Position, x, x >> 7, data);
                                //Console.WriteLine();
                            }
                            if (y == 0) //backtrack entry
                            {
                                x >>= 7; //short's data
                                y = (1L << (int)z) - 1; //bitmask
                                count = Helper(ref data, count, @in, streamOffset, ref pos, header);

                                //var shiftyY = (data >> 10);
                                y = (long)((data >> 10) & (ulong)y);

                                x += y;
                                x = large[x + 0x400];
                                y = x & 0xF;

                                //_logger?.LogTrace("count: {Count}; pos: {Pos}; data: 0x{Data:x8}; x: {X}; y: {Y}; z: {Z};", count, pos, data, x, y, z);

                            } //end backtrack entry

                            data >>= (int)y;
                            count -= (uint)y;
                            y = 0;
                            x >>= 7; //data only

                            if (x < 0x100)
                            {
                                result[arrayIndex++] = (byte)x;

                                if (arrayIndex > header.DecompressedSize)
                                    return result[..arrayIndex];
                            }
                            else if (x > 0x100) //copy previous
                            {
                                z = table2[x - 0x101];
                                if (z != 0) //segment
                                {
                                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                    y = (1 << (int)z) - 1; //mask
                                    y = (int)data & y;
                                    data >>= (int)z;
                                    count -= (uint)z;
                                } //end segment

                                z = table1[x - 0x101]; //decodeTable1[x - 0x101];
                                num = (uint)(z + y + 3);
                                count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                x = (int)(data & 0xFF);
                                x = smallArray[x];

                                y = x & 0xF; //y=normal bitcount
                                z = (int)(x & 0x70) >> 4; //z=backtrack bitcount
                                if (y == 0) //backtrack entry
                                {
                                    x >>= 7; //short's data
                                    y = (1 << (int)z) - 1; //bitmask
                                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                    y = ((int)data >> 8) & y;
                                    x += y;
                                    x = smallArray[x + 0x100];
                                    y = x & 0xF;
                                } //end backtrack entry

                                data >>= (int)y;
                                count -= y;

                                /*pull number of bits*/
                                y = 0;
                                x >>= 7;
                                z = table4[x];
                                if (z != 0) //segment
                                {
                                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                    y = (int)data & ((1 << (int)z) - 1);
                                    data >>= (int)z;
                                    count -= (uint)z;
                                } //end segment

                                z = table3[x];
                                back = (uint)(z + y + 1);

                                /*copy run*/
                                for (x = 0; num > 0; num--)
                                {
                                    z = arrayIndex - back;

                                    if (z < 0 || z >= arrayIndex)
                                        x = 0;
                                    else
                                        x = result[arrayIndex - (int)back];

                                    result[arrayIndex++] = (byte)x;

                                    if (Convert.ToUInt32(arrayIndex) > header.DecompressedSize)
                                        return result[..arrayIndex]; //failsafe
                                } //end copy run
                                /*        for(x=0;num>0;num-=x)      this is faster but would need a catch
										   {x=num;                   to keep it from copying bytes that have
										   if(x>8) x=8;              not yet been written
										   fseek(out,0-back,SEEK_END);
										   fread(bits,1,x,out);
										   fseek(out,0,SEEK_END);
										   fwrite(bits,1,x,out);
										   if(ftell(out)>edl.dsize) return ftell(out);
										   }end debug-sometime-later*/
                            } //end copy previous
                        } while (x != 0x100);
                    } //mode 1
                    else //mode 0 -
                    {
                        count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                        num = (uint)(data & 0x7FFF);
                        data >>= 15;
                        count -= 15;

                        _logger?.LogTrace("mode0\tpos: {Pos}\tout: {Index}", pos, arrayIndex);

                        if (num != 0)
                        {
                            while (num > 0)
                            {
                                count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                                x = (long)(data & 0xFF);
                                data >>= 8;
                                count -= 8;
                                result[arrayIndex++] = (byte)x;
                                num--;
                            } //end while()
                        } //write bytes
                    } //mode 0

                    /*test EOF*/
                    count = Helper(ref data, count, @in, streamOffset, ref pos, header);
                    x = (int)(data & 1);
                    data >>= 1;
                    count--;
                    if (x != 0)
                    {
                        return result; //1=EOF marker
                    }
                }

                return result[..arrayIndex];
            }
            finally
            {
                Array.Resize(ref resultBuffer, 0);
                Array.Resize(ref bits, 0);
                Array.Resize(ref smallArray, 0);
                Array.Resize(ref large, 0);

                Array.Resize(ref table1, 0);
                Array.Resize(ref table2, 0);
                Array.Resize(ref table3, 0);
                Array.Resize(ref table4, 0);
            }
        }

        #endregion
    }
}
