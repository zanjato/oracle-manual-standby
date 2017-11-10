#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
powershell.exe -c get-acl recover_standby.job^|set-acl backup_oracle.job
cmd.exe /c powershell.exe[ -nol -nop -noni -ex bypass]
  -f backup_oracle.ps1[ -backup][ -call]#>
param([switch]$backup,[switch]$call)
set-strictmode -v latest
&{
  $sw=[diagnostics.stopwatch]::startnew()
  $erroractionpreference='stop'
  try{
    . .\aux_utils.ps1
    $my=set_my
    set_bsw
    mk_log
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if(!($env:oracle_sid|? $my.CE)){throw 'Нет ORACLE_SID'}
    $tnsp,$rman=gcm tnsping.exe,rman.exe
    [void][reflection.assembly]::loadwithpartialname('Oracle.DataAccess')
    dispose-after($oc=mk_oc 'user id=/;dba privilege=sysdba'){
      dispose-after($cm=$oc.createcommand()){
        $cm.commandtext=@'
select name,database_role role
from v$database
where database_role in('PRIMARY','PHYSICAL STANDBY')
'@
        dispose-after(
          $da=new-object oracle.dataaccess.client.oracledataadapter){
          $da.selectcommand=$cm
          dispose-after($tbl=new-object data.datatable){
            if($da.fill($tbl) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
            $r1=$tbl.rows[0]
            $my.db=$r1.name.tolower()
            $my.role=$r1.role.tolower()
            if($my.role -ne 'primary'){$my.role='standby'}
          }
        }
      }
    }
    $db,$role=$my['db','role']
    $nsn="$('standby','primary'|?{$_ -ne $role})_ekr"
    $out=&$tnsp $nsn
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'tnsping.exe ${nsn}'"
    }
    for(;;){
      $out|
      ?{$_ -match '\(\s*host\s*=\s*(\w+(?:-+\w+)*(?:\.\w+(?:-+\w+)*)*)\s*\)'}|
      %{break}
      throw $out -join $my.LNW
    }
    $svr=[net.dns]::gethostentry($matches[1]).hostname
    $bkp="\\${svr}\archivedlogs\backup\%d-${role}-%T-%t-%s-"
    if($role -eq 'primary'){
      $scr=@"
configure controlfile autobackup off;
configure retention policy to recovery window of 3 days;
crosscheck backup;
delete noprompt expired backup;
crosscheck copy;
delete noprompt expired copy;
run{
  set command id to '${db} ${role} db daily full backup';
  backup as compressed backupset check logical
    database
      format '${bkp}db.bkp'
#    current controlfile
#      format '${bkp}cf.bkp'
#    spfile
#      format '${bkp}spf.bkp'
    plus archivelog
      format '${bkp}arc.bkp'
      delete input;
}
delete noprompt obsolete;
"@
    }else{
      $scr=@"
configure controlfile autobackup off;
crosscheck backup;
delete noprompt expired backup;
crosscheck copy;
delete noprompt expired copy;
run{
  set command id to '${db} ${role} db daily full backup';
  backup as compressed backupset check logical
    database
      format '${bkp}db.bkp'
    plus archivelog
      format '${bkp}arc.bkp';
}
delete noprompt obsolete recovery window of 2 days;
#delete noprompt obsolete redundancy=2;
"@
    }
    if($my.tran){stop-transcript|log;$my.tran=$null}
    $log="logs\backup_${db}_${role}_$($my.dt).log"
    try{start-transcript $log -f|log;$my.tran=$true}catch{}
    log $scr
    $scr="set echo on;{1}{0}{1}exit{1}" -f $scr,$my.LNW
    if($backup){$scr|&$rman target / nocatalog|log}
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'rman.exe'"
    }
    if(!$call){log "Затрачено '$($sw.elapsed)'";exit 0}
  }catch{
    if($call){throw}
    else{
      $x=$_|out-string
      if(test-path variable:my){$x|log -err}else{$x|write-warning}
      exit 1
    }
  }finally{
    if(test-path variable:my){
      if($my.tran){stop-transcript >$null}
      if($my.bsw){set_bsw $my.bsw}
    }
  }
}
