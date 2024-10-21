#include <iostream>
#include <vector>
#include <cassert>

// Varbyte encoding function
std::vector<uint8_t> varbyteEncode(int number)
{
    std::vector<uint8_t> encoded;
    while (number >= 128)
    {
        encoded.push_back((number & 0x7F) | 0x80);
        number >>= 7;
    }
    encoded.push_back(number & 0x7F);
    return encoded;
}

// Varbyte decoding function
int varbyteDecode(const std::vector<uint8_t> &encoded)
{
    int number = 0;
    for (int i = encoded.size() - 1; i >= 0; --i)
    {
        number = (number << 7) | (encoded[i] & 0x7F);
    }
    return number;
}

// Test function
void testVarbyteCodec()
{
    std::vector<int> test_numbers = {0, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152, 268435455};

    for (int num : test_numbers)
    {
        std::vector<uint8_t> encoded = varbyteEncode(num);
        std::cout << "Encoded: ";
        for (uint8_t byte : encoded)
        {
            std::cout << static_cast<int>(byte) << " ";
        }
        std::cout << std::endl;
        int decoded = varbyteDecode(encoded);

        std::cout << "Original: " << num << ", Encoded size: " << encoded.size()
                  << " bytes, Decoded: " << decoded << std::endl;

        assert(num == decoded && "Encoding/decoding mismatch");
    }

    std::cout << "All tests passed successfully!" << std::endl;
}

int main()
{
    testVarbyteCodec();
    return 0;
}
