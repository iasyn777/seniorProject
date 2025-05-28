def decode_guid_strict(b: bytes) -> str:
    if len(b) != 16:
        raise ValueError("Ожидано 16 байт")

    return (
        f"{b[12]:02x}{b[13]:02x}{b[14]:02x}{b[15]:02x}-"  # 20 dd c7 07
        f"{b[10]:02x}{b[11]:02x}-"  # 13 02
        f"{b[8]:02x}{b[9]:02x}-"  # 11 f0
        f"{b[0]:02x}{b[1]:02x}-"  # 9a 6e
        f"{b[2]:02x}{b[3]:02x}{b[4]:02x}{b[5]:02x}{b[6]:02x}{b[7]:02x}"  # 00 0c 29 19 e5 6d
    )


hex_str = "9A6E000C2919E56D11F0130220DDC708"
bin_data = bytes.fromhex(hex_str)

print("Ожидаемый GUID:  20ddc708-1302-11f0-9a6e-000c2919e56d")
print("Декодированный:  ", decode_guid_strict(bin_data))
