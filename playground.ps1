# Define the server address and port
$serverAddress = "127.0.0.1"
$port = 6379

# Define the messages with proper escaping
$messages = @(
    "*3`r`n`$3`r`nSET`r`n`$5`r`napple`r`n`$9`r`nblueberry`r`n", # SET command
    "*3`r`n`$4`r`nWAIT`r`n`$1`r`n1`r`n`$3`r`n500`r`n"           # WAIT command
)

# Create the TCP client and connect
$tcpClient = New-Object System.Net.Sockets.TcpClient
try {
    $tcpClient.Connect($serverAddress, $port)
    if ($tcpClient.Connected) {
        Write-Output "Connected to $serverAddress on port $port"
        $stream = $tcpClient.GetStream()

        # Loop through each message in the array
        foreach ($message in $messages) {
            # Convert the message to bytes
            $messageBytes = [System.Text.Encoding]::ASCII.GetBytes($message)

            # Send the message
            Write-Output "Sending message: $message"
            $stream.Write($messageBytes, 0, $messageBytes.Length)

            # Optional: Read and print the response
            $buffer = New-Object byte[] 1024
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            $response = [System.Text.Encoding]::ASCII.GetString($buffer, 0, $bytesRead)
            Write-Output "Response: $response"
        }

        # Close the stream
        $stream.Close()
    } else {
        Write-Output "Could not connect to the server."
    }
} catch {
    Write-Output "Error: $_"
} finally {
    $tcpClient.Close()
}
