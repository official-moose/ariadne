import sys
from mm.utils.helpers.wintermute import send_alert

def main():
    print("Send Alert Function Test...")

    try:
        print(f"About to call send_alert with process_name='naomi'") 
        send_alert(
            subject="This is some bullshit.",
            message="This email is supposed to be from Naomi, but it likely arrived from Wintermute. Three AIs and 2 hours later, still no idea why.",
            process_name="naomi"
        )
        print("Send Success")
    except Exception as e:
        print(f"Send Failure: {e}") 
        sys.exit(1)

if __name__ == "__main__": 
    main()
