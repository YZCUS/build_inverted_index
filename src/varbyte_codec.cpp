#include <iostream>
#include <vector>
#include <fstream>
#include <cassert>

// Varbyte encode function
std::vector<uint8_t> varbyteEncode(int number, int &size)
{
    std::vector<uint8_t> bytes;
    while (number >= 128)
    {
        bytes.push_back((number & 0x7F) | 0x80);
        number >>= 7;
    }
    bytes.push_back(number & 0x7F);
    size = bytes.size();
    return bytes;
}

// Varbyte decode function
int varbyteDecode(const std::vector<uint8_t> &encoded)
{
    int number = 0;
    int shift = 0;

    for (size_t i = 0; i < encoded.size(); ++i)
    {
        number |= (encoded[i] & 0x7F) << shift;
        if (!(encoded[i] & 0x80))
            break;
        shift += 7;
    }

    return number;
}

// write encoded data to file
void writeEncodedToFile(const std::vector<uint8_t> &encoded, std::ofstream &outFile)
{
    outFile.write(reinterpret_cast<const char *>(encoded.data()), encoded.size());
}

// read encoded data from file
std::vector<uint8_t> readEncodedFromFile(std::ifstream &inFile)
{
    std::vector<uint8_t> encoded;
    uint8_t byte;
    do
    {
        inFile.read(reinterpret_cast<char *>(&byte), 1);
        if (inFile.gcount() == 0)
            break;
        encoded.push_back(byte);
    } while (byte & 0x80);
    return encoded;
}

// test function
void testVarbyteCodec()
{
    std::vector<int> test_numbers = {0, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152, 268435455};

    // write test
    std::ofstream outFile("encoded_numbers.bin", std::ios::binary);
    std::vector<int> sizes;
    for (int num : test_numbers)
    {
        int size;
        auto encoded = varbyteEncode(num, size);
        sizes.push_back(size);
        writeEncodedToFile(encoded, outFile);
    }
    outFile.close();

    // read and decode test
    std::ifstream inFile("encoded_numbers.bin", std::ios::binary);
    for (int num : test_numbers)
    {
        auto encoded = readEncodedFromFile(inFile);
        int decoded = varbyteDecode(encoded);

        std::cout << "original value: " << num << ", encoding size: " << encoded.size()
                  << " bytes, decoded value: " << decoded << std::endl;

        assert(num == decoded && "encoding/decoding mismatch");
    }
    inFile.close();

    std::cout << "all tests passed!" << std::endl;
}

int main()
{
    testVarbyteCodec();
    return 0;
}
