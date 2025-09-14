#!/usr/bin/env python3
"""
Display all ANSI color codes
"""

# Regular colors
colors = {
    'Black': '\033[30m',
    'Red': '\033[31m',
    'Green': '\033[32m',
    'Yellow': '\033[33m',
    'Blue': '\033[34m',
    'Magenta': '\033[35m',
    'Cyan': '\033[36m',
    'White': '\033[37m',
    'Default': '\033[39m',
    
    # Bright colors
    'Bright Black': '\033[90m',
    'Bright Red': '\033[91m',
    'Bright Green': '\033[92m',
    'Bright Yellow': '\033[93m',
    'Bright Blue': '\033[94m',
    'Bright Magenta': '\033[95m',
    'Bright Cyan': '\033[96m',
    'Bright White': '\033[97m',
}

# Background colors
bg_colors = {
    'BG Black': '\033[40m',
    'BG Red': '\033[41m',
    'BG Green': '\033[42m',
    'BG Yellow': '\033[43m',
    'BG Blue': '\033[44m',
    'BG Magenta': '\033[45m',
    'BG Cyan': '\033[46m',
    'BG White': '\033[47m',
    'BG Default': '\033[49m',
    
    # Bright backgrounds
    'BG Bright Black': '\033[100m',
    'BG Bright Red': '\033[101m',
    'BG Bright Green': '\033[102m',
    'BG Bright Yellow': '\033[103m',
    'BG Bright Blue': '\033[104m',
    'BG Bright Magenta': '\033[105m',
    'BG Bright Cyan': '\033[106m',
    'BG Bright White': '\033[107m',
}

# Styles
styles = {
    'Bold': '\033[1m',
    'Dim': '\033[2m',
    'Italic': '\033[3m',
    'Underline': '\033[4m',
    'Blink': '\033[5m',
    'Reverse': '\033[7m',
    'Hidden': '\033[8m',
    'Strikethrough': '\033[9m',
}

RESET = '\033[0m'

print("\nFOREGROUND COLORS")
print("="*50)
for name, code in colors.items():
    print(f"{code}{name:<20} {code} ■■■■■ Sample Text{RESET}")

print("\nBACKGROUND COLORS")
print("="*50)
for name, code in bg_colors.items():
    print(f"{code}{name:<20} {code} ■■■■■ Sample Text{RESET}")

print("\nSTYLES")
print("="*50)
for name, code in styles.items():
    print(f"{code}{name:<20} {code} Sample Text{RESET}")

print("\nCOMBINATIONS")
print("="*50)
print(f"\033[1;32mBold Green{RESET}")
print(f"\033[4;91mUnderlined Bright Red{RESET}")
print(f"\033[42;97mWhite on Green Background{RESET}")
print(f"\033[1;3;33mBold Italic Yellow{RESET}")
print(f"\033[5;35mBlinking Magenta{RESET}")

print("\nUSEFUL CODES")
print("="*50)
print(f"Reset: \\033[0m")
print(f"Your Green: \\033[92m (Bright Green)")
print(f"Your Dark Green: \\033[32m (Green)")
print(f"Your Light Gray: \\033[37m (White/Light Gray)")
print(f"Your Red: \\033[91m (Bright Red)")