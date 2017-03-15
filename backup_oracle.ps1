#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
get-acl (join-path $env:windir\system32\tasks manual_standby_recovery.job)|
  set-acl (join-path $env:windir\system32\tasks backup_oracle.job)
powershell.exe -nol -nop -non -f backup_oracle.ps1[ -backup][ -call]#>
param([switch]$backup,[switch]$call)
set-strictmode -vers latest
&{
  function set_my{
    @{LNW=[environment]::newline
      NL=@{str="`n";ach=[char[]]"`n"}
      REE=[stringsplitoptions]::removeemptyentries
      CE={$_ -and $_.trim()}
      LLB={date -f 'yyyy-MM-dd HH:mm:ss.fffffff'},{' '*27}
      bsw=$null
      tran=$null}
  }
  function set_bsw{param([validatenotnull()][int]$bsw=512)
    $raw=$host.ui.rawui
    $bs=$raw.buffersize
    $bsw,$bs.width=$bs.width,$bsw
    $raw.buffersize=$bs
    $my.bsw=$bsw
  }
  function log{param([parameter(valuefrompipeline=$true)][string]$log,
                     [switch]$err)
    process{
      $log|? $my.CE|%{$_.replace($my.LNW,$my.NL.str).split($my.NL.ach,$my.REE)}|
      ? $my.CE|%{$i=0}{"$(&$my.LLB[$i]) $(if($err){'!!'}else{'--'}) $_";$i=1}|
      out-host
    }
  }
  function mk_log{
    $dt=date
    $my.dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
    $my.sn=$myinvocation.scriptname
    $my.log=[io.path]::getfilenamewithoutextension($my.sn)
    $my.log="$(split-path $my.sn)\logs\$($my.log)_$($my.dt).log"
    try{start-transcript $my.log -f|log;$my.tran=$true}catch{}
  }
  function dispose-after{
    param([validatenotnull()][object]$obj,[validatenotnull()][scriptblock]$sb)
    try{&$sb}
    finally{
      if($obj -is [idisposable] -or $obj -as [idisposable]){
        [void][idisposable].getmethod('Dispose').invoke($obj,$null)
      }
    }
  }
  function mk_oc{param([validatenotnullorempty()][string]$cs)
    $oc=new-object oracle.dataaccess.client.oracleconnection $cs
    $oc.open()
    $oc
  }
  $sw=[diagnostics.stopwatch]::startnew()
  $erroractionpreference='stop'
  $my=set_my
  try{
    set_bsw
    mk_log
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if($env:oracle_sid|?{!$_ -or !$_.trim()}){throw 'Нет ORACLE_SID'}
#    $env:path="C:\oracle\product\10.2.0\db_1\BIN;${env:path}"
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
    $my.log="$(split-path $my.log)\backup_${db}_${role}_$($my.dt).log"
    try{start-transcript $my.log -f|log;$my.tran=$true}catch{}
    log $scr
    $scr="set echo on;{1}{0}{1}exit{1}" -f $scr,$my.LNW
    if($backup){$scr|&$rman target / nocatalog|log}
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'rman.exe'"
    }
    if(!$call){log "Затрачено '$($sw.elapsed)'";exit 0}
  }catch{if($call){throw}else{$_|out-string|log -err;exit 1}}
   finally{
     if($my.tran){stop-transcript >$null}
     if($my.bsw){set_bsw $my.bsw}
   }
}
