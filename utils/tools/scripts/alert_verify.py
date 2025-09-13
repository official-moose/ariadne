import sys
from mm.utils.helpers.wintermute import send_alert

def main():
    print("Send Alert Function Test...")

    try:
        send_alert(
            subject="Testing the send alert function via Wintermute",
            message="This is just a test. Send alert function has been invoked from Wintermute.",
            process_name="alert_verify"
        )
        print("Send Success")
    except Exception as e:
        print(f"Send Failure: {e}")
        sys.exit(1)

if __name__ == "__main__": 
    main()
