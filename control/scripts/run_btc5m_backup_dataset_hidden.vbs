Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
controlDir = fso.GetParentFolderName(scriptDir)
repoRoot = fso.GetParentFolderName(controlDir)

pythonExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python311\python.exe"
If Not fso.FileExists(pythonExe) Then
    pythonExe = "python"
End If

command = "cmd.exe /c cd /d """ & repoRoot & """ && """ & pythonExe & """ ""scripts\btc5m_backup_dataset.py"""
exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode
