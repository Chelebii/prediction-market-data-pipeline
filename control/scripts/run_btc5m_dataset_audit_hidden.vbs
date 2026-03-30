Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
controlDir = fso.GetParentFolderName(scriptDir)
repoRoot = fso.GetParentFolderName(controlDir)
ensureScript = repoRoot & "\control\scripts\ensure_btc5m_process_exes.ps1"
sitePackages = repoRoot & "\.venv\Lib\site-packages"
existingPythonPath = shell.Environment("PROCESS").Item("PYTHONPATH")
If Len(existingPythonPath) > 0 Then
    shell.Environment("PROCESS").Item("PYTHONPATH") = repoRoot & ";" & sitePackages & ";" & existingPythonPath
Else
    shell.Environment("PROCESS").Item("PYTHONPATH") = repoRoot & ";" & sitePackages
End If

processExe = shell.ExpandEnvironmentStrings("%BTC5M_AUDIT_EXE_PATH%")
If processExe = "%BTC5M_AUDIT_EXE_PATH%" Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Python\pythoncore-3.14-64\btc5m-dataset-audit.exe"
End If
If Not fso.FileExists(processExe) Then
    shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File """ & ensureScript & """ -Quiet", 0, True
End If
If Not fso.FileExists(processExe) Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python314\btc5m-dataset-audit.exe"
End If
If Not fso.FileExists(processExe) Then
    processExe = shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python311\btc5m-dataset-audit.exe"
End If
If Not fso.FileExists(processExe) Then
    processExe = repoRoot & "\.venv\Scripts\python.exe"
End If
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
command = """" & processExe & """ ""scripts\btc5m_audit_dataset.py"" --lookback-hours 48 --max-markets 250 --include-active"
exitCode = shell.Run(command, 0, True)
WScript.Quit exitCode
