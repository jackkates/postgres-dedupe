This is a proof of concept for one way to prevent 
consuming messages twice. It is the insert-based approach
where the processing is wrapped in a transaction.

To set up the database:
`docker-compose up -d`

To run:
`go build && ./exactlyonce`

To filter for completed messages
`go build && ./exactlyonce | grep -i "finalized"`

There is also an execution trace in the traces/ directory. 
