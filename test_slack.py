#!/usr/bin/env python3
"""
Quick test script to verify Slack bot connection and channel access.
Run this to debug your Slack setup.
"""

import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

def test_slack_connection():
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL")
    
    print("🔍 Testing Slack Connection")
    print("=" * 40)
    
    if not token:
        print("❌ SLACK_BOT_TOKEN not found in .env file")
        return
    
    if not channel:
        print("❌ SLACK_CHANNEL not found in .env file")
        return
    
    print(f"✅ Token found: {token[:12]}...")
    print(f"✅ Channel: {channel}")
    print()
    
    # Test 1: Basic bot connection
    print("🔗 Testing bot authentication...")
    client = WebClient(token=token)
    
    try:
        auth_response = client.auth_test()
        bot_name = auth_response.get("user", "Unknown")
        team_name = auth_response.get("team", "Unknown")
        print(f"✅ Bot authenticated: @{bot_name} in {team_name}")
    except SlackApiError as e:
        print(f"❌ Bot authentication failed: {e.response['error']}")
        return
    
    # Test 2: List channels bot has access to
    print("\n📋 Channels bot can access:")
    try:
        channels_response = client.conversations_list(types="public_channel,private_channel")
        channels = channels_response.get("channels", [])
        
        for ch in channels[:10]:  # Show first 10
            ch_id = ch.get("id")
            ch_name = ch.get("name", "unknown")
            is_member = ch.get("is_member", False)
            print(f"   {'✅' if is_member else '❌'} #{ch_name} ({ch_id})")
        
        if len(channels) > 10:
            print(f"   ...and {len(channels) - 10} more channels")
            
    except SlackApiError as e:
        print(f"❌ Failed to list channels: {e.response['error']}")
    
    # Test 3: Try to send a test message
    print(f"\n📤 Testing message to {channel}...")
    try:
        test_response = client.chat_postMessage(
            channel=channel,
            text="🧪 Test message from scraper bot - setup successful!"
        )
        print(f"✅ Test message sent successfully!")
        print(f"   Message ID: {test_response['ts']}")
        
    except SlackApiError as e:
        error = e.response['error']
        print(f"❌ Failed to send message: {error}")
        
        if error == "channel_not_found":
            print("\n💡 Solutions for 'channel_not_found':")
            print("   1. Double-check the channel ID is correct")
            print("   2. Make sure the bot is added to the channel")
            print("   3. Try using channel name like #n8n instead")
            print("   4. Check if channel is private and bot needs invitation")
        
        elif error == "not_in_channel":
            print("\n💡 Solution: Add your bot to the channel:")
            print(f"   Go to {channel} and type: /invite @{bot_name}")

    print("\n" + "=" * 40)
    print("🏁 Test completed!")

if __name__ == "__main__":
    test_slack_connection()