hex_str = "9A6E000C2919E56D11F0130220DDC707"  # <-- это ты получил из SQL
bin_data = bytes.fromhex(hex_str)

print("Всего байт:", len(bin_data))
print("Распечатка байтов по порядку:")

for i, b in enumerate(bin_data):
    print(f"{i:02}: {b:02x}")
