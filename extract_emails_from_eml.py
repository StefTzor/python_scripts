#!/usr/bin/env python3
import os
import csv
import email
import re
import argparse
from email import policy
from pathlib import Path

def extract_email_addresses(text):
    """Extract email addresses from a string using regex."""
    # This regex pattern matches most valid email addresses
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)

def is_mailer_daemon(from_address):
    """Check if the from address is a mailer daemon."""
    if not from_address:
        return False
    
    from_lower = from_address.lower()
    daemon_indicators = [
        'mailer-daemon', 
        'mail delivery system', 
        'postmaster', 
        'mail delivery subsystem',
        'mail administrator',
        'system administrator',
        'delivery status notification',
        'undeliverable',
        'returned mail'
    ]
    
    return any(indicator in from_lower for indicator in daemon_indicators)

def find_failed_recipient(msg):
    """
    For bounce emails, find the email address that failed delivery.
    This is the recipient address that couldn't be delivered to.
    """
    # First, check for standard failure notification headers
    for header in ['X-Failed-Recipients', 'Original-Recipient', 'Final-Recipient']:
        if header in msg:
            header_value = msg.get(header, '')
            emails = extract_email_addresses(header_value)
            if emails:
                return emails[0]  # Return the first found email
    
    # Check the message body for common bounce patterns
    content = ""
    
    # Get content from all text parts
    if msg.is_multipart():
        for part in msg.iter_parts():
            content_type = part.get_content_type()
            if content_type == 'text/plain' or content_type == 'text/html':
                try:
                    content += part.get_content() + "\n"
                except:
                    pass
    else:
        try:
            content = msg.get_content()
        except:
            pass
    
    # Common patterns in bounce messages for failed recipients
    # The order is important - more specific patterns first
    patterns = [
        r'(?:Failed recipient|Failed address|Failed Recipient|Recipient address)[^\w@]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'(?:The following address(?:es)? failed|failure notice for|failed to deliver to)[^\w@]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'Your message to ([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}) couldn\'t be delivered',
        r'(?:originally addressed to|intended for|addressed to|sent to)[^\w@]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'(?:recipient|Recipient|RCPT TO):[^\w@]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>:',  # Common format in status reports
        r'(?:mailbox|account|user|email address)[^\w@]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})[^\w@]*(?:not found|doesn\'t exist|is full|over quota|rejected)'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, content)
        if matches:
            return matches[0]
    
    # If we still don't have a match, look for any email in the subject line
    # (sometimes the subject contains "Mail delivery failed: returning message to sender <email>")
    subject = msg.get('Subject', '')
    subject_emails = extract_email_addresses(subject)
    if subject_emails:
        return subject_emails[0]
    
    # Last resort: Get the first email address found in the content
    all_emails = extract_email_addresses(content)
    # Filter out common sender addresses that might appear in bounce messages
    filtered_emails = [e for e in all_emails if not (
        'mailer-daemon' in e.lower() or 
        'postmaster' in e.lower() or
        'mail-delivery' in e.lower()
    )]
    
    if filtered_emails:
        return filtered_emails[0]
    
    return None

def process_eml_file(file_path):
    """Process an .eml file and extract relevant email addresses."""
    try:
        with open(file_path, 'r', errors='ignore') as f:
            msg = email.message_from_file(f, policy=policy.default)
        
        from_address = msg.get('From', '')
        is_daemon = is_mailer_daemon(from_address)
        
        # Initialize result
        result = {
            'file': Path(file_path).name,
            'is_bounce': 'Yes' if is_daemon else 'No',
            'failed_recipient': None,
            'original_sender': None
        }
        
        # For bounce messages, find the failed recipient
        if is_daemon:
            # Get the failed recipient (the address that bounced)
            failed_recipient = find_failed_recipient(msg)
            result['failed_recipient'] = failed_recipient
            
            # Try to find original sender (sometimes available in bounce messages)
            # This could be in Reply-To or Return-Path headers
            for header in ['Reply-To', 'Return-Path']:
                if header in msg:
                    emails = extract_email_addresses(msg.get(header, ''))
                    if emails:
                        result['original_sender'] = emails[0]
                        break
        else:
            # For regular emails, get the To address
            to_addresses = extract_email_addresses(msg.get('To', ''))
            if to_addresses:
                result['failed_recipient'] = to_addresses[0]
            
            # Get the From address
            from_emails = extract_email_addresses(from_address)
            if from_emails:
                result['original_sender'] = from_emails[0]
        
        return result
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return {
            'file': Path(file_path).name,
            'is_bounce': 'Unknown',
            'failed_recipient': None,
            'original_sender': None
        }

def main():
    parser = argparse.ArgumentParser(description='Extract failed recipient emails from .eml files')
    parser.add_argument('folder', help='Folder containing .eml files')
    parser.add_argument('-o', '--output', default='failed_emails.csv', 
                        help='Output CSV file (default: failed_emails.csv)')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Print detailed processing information')
    args = parser.parse_args()
    
    folder_path = Path(args.folder)
    output_file = args.output
    verbose = args.verbose
    
    if not folder_path.exists() or not folder_path.is_dir():
        print(f"Error: {folder_path} is not a valid directory")
        return

    # Process all .eml files
    results = []
    processed_files = 0
    bounce_files = 0
    
    print(f"Processing .eml files in {folder_path}...")
    
    for file in folder_path.glob('**/*.eml'):
        if verbose:
            print(f"Processing: {file.name}")
        
        result = process_eml_file(file)
        
        if result['is_bounce'] == 'Yes':
            bounce_files += 1
            
        if verbose:
            print(f" - Bounce: {result['is_bounce']}")
            print(f" - Failed recipient: {result['failed_recipient']}")
            print(f" - Original sender: {result['original_sender']}")
        
        if result['failed_recipient']:
            results.append(result)
        
        processed_files += 1
        
        # Print progress every 100 files
        if processed_files % 100 == 0 and not verbose:
            print(f"Processed {processed_files} files...")
    
    # Write results to CSV
    if results:
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['file', 'is_bounce', 'failed_recipient', 'original_sender']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        print(f"\nExtraction complete!")
        print(f"Processed {processed_files} .eml files")
        print(f"Found {bounce_files} bounce messages")
        print(f"Found {len(results)} failed recipient email addresses")
        print(f"Results saved to {output_file}")
    else:
        print("\nNo failed recipient email addresses were found in the processed files.")

if __name__ == '__main__':
    main()