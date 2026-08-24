!macro customInstall
  ; Program Files is read-only for normal users. Create the required host
  ; hardlink while the installer still has permission to write to $INSTDIR.
  Delete "$INSTDIR\WeFlow.exe"
  nsExec::ExecToLog '"$SYSDIR\cmd.exe" /D /C mklink /H "$INSTDIR\WeFlow.exe" "$INSTDIR\Weport.exe"'
!macroend
