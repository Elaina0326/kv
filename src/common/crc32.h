#pragma once

#include <cstddef>
#include <cstdint>

namespace raftkv
{

    inline const uint32_t *Crc32Table()
    {
        static const uint32_t *table = []
        {
            static uint32_t t[256];
            for (uint32_t i = 0; i < 256; ++i)
            {
                uint32_t c = i;
                for (int k = 0; k < 8; ++k)
                    c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                t[i] = c;
            }
            return t;
        }();
        return table;
    }

    inline uint32_t Crc32(const void *data, size_t len)
    {
        const uint8_t *p = static_cast<const uint8_t *>(data);
        const uint32_t *table = Crc32Table();
        uint32_t crc = 0xFFFFFFFFu;
        for (size_t i = 0; i < len; ++i)
            crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }

    inline uint32_t Crc32(const char *data, size_t len) { return Crc32(static_cast<const void *>(data), len); }

} 
