$global:kafkaWindowsBatFolder = "C:\Apache\confluent-5.2.1-2.12\bin\windows\"
$global:kafkaAndZooLoggingFolder = "C:\temp\"


$global:kafkaTopics = 
	"rating-submit-topic"
	
$global:ProcessesToKill = @()



function RunPipeLine() 
{
	WriteHeader "STOPPING PREVIOUS SERVICES"
	StopZookeeper
	StopKafka
	
	$path = $kafkaAndZooLoggingFolder + "kafka-logs"
	Remove-Item -Recurse -Force $path
	
	$path = $kafkaAndZooLoggingFolder + "zookeeper"
	Remove-Item -Recurse -Force $path
	
	

	Start-Sleep -s 20
	
	WriteHeader "STARTING NEW SERVICE INSTANCES"
	StartZookeeper
	
	Start-Sleep -s 20
	StartKafka
	
	Start-Sleep -s 20

	CreateKafkaTopics
    
	Start-Sleep -s 20
	
	
	WaitForKeyPress

	WriteHeader "KILLING PROCESSES CREATED BY SCRIPT"
	KillProcesses
}

function WriteHeader($text) 
{
	Write-Host "========================================`r`n"
	Write-Host "$text`r`n"
	Write-Host "========================================`r`n"
}


function StopZookeeper() {
    # C:\Apache\confluent-5.2.1-2.12\bin\windows\zookeeper-server-stop.bat
	$zookeeperCommandLine = $global:kafkaWindowsBatFolder + "zookeeper-server-stop.bat"
	Write-Host "> Zookeeper Command Line : $zookeeperCommandLine`r`n"
    $global:ProcessesToKill += start-process $zookeeperCommandLine -WindowStyle Normal -PassThru
}

function StopKafka() {
	# C:\Apache\confluent-5.2.1-2.12\bin\windows\kafka-server-stop.bat
	$kafkaServerCommandLine = $global:kafkaWindowsBatFolder + "kafka-server-stop.bat" 
	Write-Host "> Kafka Server Command Line : $kafkaServerCommandLine`r`n"
    $global:ProcessesToKill += start-process $kafkaServerCommandLine  -WindowStyle Normal -PassThru
}

function StartZookeeper() {
    # C:\Apache\confluent-5.2.1-2.12\bin\windows\zookeeper-server-start.bat C:\Apache\confluent-5.2.1-2.12\bin\windows\..\..\etc\kafka\zookeeper.properties
	$zookeeperCommandLine = $global:kafkaWindowsBatFolder + "zookeeper-server-start.bat"
	$arguments = $global:kafkaWindowsBatFolder + "..\..\etc\kafka\zookeeper.properties"
	Write-Host "> Zookeeper Command Line : $zookeeperCommandLine args: $arguments `r`n"
    $global:ProcessesToKill += start-process $zookeeperCommandLine $arguments -WindowStyle Normal -PassThru
}

function StartKafka() {
    # C:\Apache\confluent-5.2.1-2.12\bin\windows\kafka-server-start.bat C:\Apache\confluent-5.2.1-2.12\bin\windows\..\..\etc\kafka\server.properties
	$kafkaServerCommandLine = $global:kafkaWindowsBatFolder + "kafka-server-start.bat" 
	$arguments = $global:kafkaWindowsBatFolder + "..\..\etc\kafka\server.properties"
	Write-Host "> Kafka Server Command Line : $kafkaServerCommandLine args: $arguments `r`n"
    $global:ProcessesToKill += start-process $kafkaServerCommandLine $arguments -WindowStyle Normal -PassThru
}

function CreateKafkaTopics() 
{
   # C:\Apache\confluent-5.2.1-2.12\bin\windows\kafka-topics.bat --zookeeper localhost:2181 --delete --topic rating-submit-topic
	  
   # C:\Apache\confluent-5.2.1-2.12\bin\windows\kafka-topics.bat --zookeeper localhost:2181 --create  --replication-factor 1 --partitions 1 --topic rating-submit-topic
	Foreach ($topic in $global:kafkaTopics )
	{
		$kafkaCommandLine = $global:kafkaWindowsBatFolder + "kafka-topics.bat"
		$arguments = "--zookeeper localhost:2181 --create  --replication-factor 1 --partitions 1 --topic $topic"
		Write-Host "> Create Kafka Topic Command Line : $kafkaCommandLine args: $arguments `r`n"
		$global:ProcessesToKill += start-process $kafkaCommandLine $arguments -WindowStyle Normal -PassThru
	}
}

function WaitForKeyPress
{
	Write-Host -NoNewLine "Press any key to continue....`r`n"
	[Console]::ReadKey()
}


function KillProcesses() 
{
	Foreach ($processToKill in $global:ProcessesToKill )
	{
		$name = $processToKill | Get-ChildItem -Name
		Write-Host "Killing Process : $name `r`n" 
		$processToKill | Stop-Process -Force
	}
}


# Kick of the entire pipeline
RunPipeLine