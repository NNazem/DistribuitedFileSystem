This project is a distributed file system that allows uploading, storing, and retrieving files across multiple nodes. A central server manages file distribution to nodes and uses Redis to store metadata related to file locations. Files are distributed across nodes based on available storage, and each node can receive and retrieve files upon request from the central server.

Technologies Used

	•	Go: Main language for implementing the central server and nodes.
	•	Redis: Used for metadata storage (file-node mapping).
	•	HTTP & Gorilla Mux: For handling HTTP endpoints for the server and nodes.
	•	SHA-256: Hashing algorithm for unique file identification.
	•	Local File System: Each node uses its local file system to store files physically.

Implemented Features

	File Upload and Load Balancing Across Nodes
	  •	The central server receives a file and distributes it to the node with the lowest available storage.
	  •	The mapping between the file hash and node address is stored in Redis.
	File Name Hashing
	  •	The file name is hashed with SHA-256 to generate a unique identifier, which is then used to store metadata in Redis.
	Metadata Storage in Redis
	  •	File metadata is stored in Redis, allowing the system to track which node holds each file.
	File Download
	  •	The central server can retrieve specific files using Redis to identify the node where each file is stored, then sending the file back to the client.
	Node Capacity Monitoring
  	•	The system monitors each node’s available storage and uses this data to balance the load efficiently.
	Node Services for File Handling
	  •	Each node provides endpoints to receive, retrieve, and check for file existence, as well as calculate storage usage.

This setup provides a simple but effective distributed system, with centralized file management and fault tolerance facilitated through Redis.
