from JSONTokenizer import tokenize, compute_word_frequencies
import math

def _leftRotate(x, amount):
    """
    Took from https://github.com/Utkarsh87/md5-hashing/blob/master/md5.py
    """
    x &= 0xFFFFFFFF
    return (x << amount | x >> (32-amount)) & 0xFFFFFFFF

def _md5(s):
    """
    Algorithm: https://en.wikipedia.org/wiki/MD5
    Made with reference to https://github.com/Utkarsh87/md5-hashing/blob/master/md5.py
    """
    original_length = len(s)
    s = bytearray(s.encode('ascii', errors='replace'))

    # Constants found in md5 algorithm
    shift = [7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22, 
             7, 12, 17, 22, 5,  9, 14, 20,  5,  9, 14, 20, 
             5,  9, 14, 20,  5,  9, 14, 20, 4, 11, 16, 23, 
             4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,
             6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21, 
             6, 10, 15, 21]
    
    k = []
    for i in range(64):
        k.append(math.floor((2 ** 32) * abs(math.sin(i + 1))))
    
    a0 = 0x67452301
    b0 = 0xefcdab89
    c0 = 0x98badcfe
    d0 = 0x10325476
    
    # Adding padding
    s.append(0x80)  # Append '1' bit followed by 7 zero bits
    while len(s) % 64 != 56:
        s.append(0)
    
    # Adding Length
    s.extend((original_length * 8).to_bytes(8, 'little'))

    # For each 64 bit chunk, run the md5 algorithm
    for j in range(0, len(s), 64):
        chunk = s[j:j + 64]
    
        a = a0
        b = b0
        c = c0
        d = d0

        # Running each of the 4 functions from md5 16 times each
        for i in range(64):
            f = 0
            g = 0
            if i < 16:
                f = (b & c) | ((~b) & d)
                g = i
            elif i < 32:
                f = (d & b) | ((~d) & c)
                g = (5 * i + 1) % 16
            elif i < 48:
                f = b ^ c ^ d
                g = (3 * i + 5) % 16
            elif i < 64:
                f = c ^ (b | (~d))
                g = (7 * i) % 16
    
            Mg = int.from_bytes(chunk[4 * g:4 * g + 4], 'little') # Extracting part of the message to use in the updating part

            f = f + a + Mg + k[i]
            a = d
            d = c
            c = b
            b = (b + _leftRotate(f, shift[i])) % (2 ** 32)

        # Adding to previous values mod 2 ** 32 so we only get 32 bit numbers 
        a0 = (a0 + a) % (2 ** 32)
        b0 = (b0 + b) % (2 ** 32)
        c0 = (c0 + c) % (2 ** 32)
        d0 = (d0 + d) % (2 ** 32)

    # Slotting each number in place
    hash_value = a0
    hash_value += b0 << 32
    hash_value += c0 << 32 * 2
    hash_value += d0 << 32 * 3
    
    return hash_value.to_bytes(16, 'little')

def hash(s):
    """Hashes given string into bytes"""
    return _md5(s)

def simhash(phrase):
    """
    Returns the hash of the given phrase in bytes using the simhash algorithm. The length is the same size as
    the hashing algorithm used (md5) so 16 bytes. Features of this simhash are the tokens in the phrase
    and the weights are the frequency of each token.

    Arguments:
        phrase: str - the string to be hashed via simhash.

    Returns:
        Hash value - bytes
    """

    # tokenize phrase and count words
    count_map = compute_word_frequencies(tokenize(phrase))
    
    # hashing
    # not important but will be stored little endian. i.e summed_bits[0] is 2^0
    summed_bits = [] 

    hash_size = 0
    for token, count in count_map.items():
        hashed_string = hash(token)
        hash_size = len(hashed_string)
        mask = 1

        # for every bit, multiply by weight(count) and add to summed_bits
        for j in range(hash_size * 8):
            if j >= len(summed_bits):
                summed_bits.append(0)
            
            bit = mask & int.from_bytes(hashed_string)
            summed_bits[j] += count * (-1 if bit == 0 else 1)
            mask = mask << 1 # move to next bit

    # converting sums to 1s and 0s
    for i, count in enumerate(summed_bits):
        if count <= 0:
            summed_bits[i] = 0
        else:
            summed_bits[i] = 1

    # converting bit vector to bytes
    hash_value = 0
    for bit in summed_bits:
        hash_value = (hash_value << 1) | bit

    return hash_value.to_bytes(hash_size)

def distance(hash1, hash2):
    """
    Calculates the number of different bits in each hash (Hamming distance)

    Arguments:
        hash1: bytes
        hash2: bytes

    Returns:
        count: int
    """
    if len(hash1) != len(hash2):
        raise ValueError(f'Hash1 {hash1} and Hash2 {hash2} are not the same sizes')

    return sum(bin(a ^ b).count('1') for a, b in zip(hash1, hash2))

def calculate_similarity_score(hash1, hash2):
     """
    Calculates percentage of bits that are the same between 2 sim hashes. They should be identical in length (i.e amount of bytes)

    Arguments:
        hash1: bytes - hash from simhash
        hash2: bytes - hash from simhash

    Returns:
        count: int
    """
    return 1 - (distance(hash1, hash2) / (len(hash1) * 8))
