Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
controlDir = fso.GetParentFolderName(scriptDir)
repoRoot = fso.GetParentFolderName(controlDir)
sitePackages = repoRoot & "\.venv\Lib\site-packages"
existingPythonPath = shell.Environment("PROCESS").Item("PYTHONPATH")
If Len(existingPythonPath) > 0 Then
    shell.Environment("PROCESS").Item("PYTHONPATH") = repoRoot & ";" & sitePackages & ";" & existingPythonPath
Else
    shell.Environment("PROCESS").Item("PYTHONPATH") = repoRoot & ";" & sitePackages
End If

processExe = repoRoot & "\.venv\Scripts\python.exe"
If Not fso.FileExists(processExe) Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Python\pythoncore-3.14-64\python.exe"
End If
If Not fso.FileExists(processExe) Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python314\python.exe"
End If
If Not fso.FileExists(processExe) Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python311\python.exe"
End If
If Not fso.FileExists(processExe) Then
    processExe = "python"
End If

shell.CurrentDirectory = repoRoot
command = """" & processExe & """ ""scripts\btc5m_build_features.py"" --lookback-hours 24 --max-markets 500"
exitCode = shell.Run(command, 0, True)
If exitCode <> 0 Then
    WScript.Quit exitCode
End If

command = """" & processExe & """ ""scripts\btc5m_build_labels.py"" --lookback-hours 24 --max-markets 500"
exitCode = shell.Run(command, 0, True)
If exitCode <> 0 Then
    WScript.Quit exitCode
End If

command = """" & processExe & """ ""scripts\btc5m_build_decision_dataset.py"" --lookback-hours 24 --max-markets 500"
exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode
