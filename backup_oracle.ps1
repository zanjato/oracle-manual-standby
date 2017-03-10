#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
get-acl (join-path $env:windir\system32\tasks manual_standby_recovery.job)|
  set-acl (join-path $env:windir\system32\tasks backup_oracle.job)
powershell.exe -nol -nop -non -f backup_oracle.ps1[ -call]#>
param([switch]$call)
set-strictmode -vers latest
&{
  function dispose(
    [parameter(mandatory=$true,valuefrompipeline=$true)]$obj){
    process{
      if($obj -is [idisposable] -or $obj -as [idisposable]){
        [void][idisposable].getmethod('Dispose').invoke($obj,$null)
      }
    }
  }
  function dispose-after($obj,[scriptblock]$sb){try{&$sb}finally{dispose $obj}}
  $sw=[diagnostics.stopwatch]::startnew()
  $erroractionpreference='stop'
  $props=@{tran=$null}
  try{
    $dt=date
    $dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
    $sn=$myinvocation.scriptname
    $log=[io.path]::getfilenamewithoutextension($sn)
    $log="$(split-path $sn)\logs\${log}_${dt}.log"
    try{
      start-transcript $log -f -outv tran|write
      $props.tran=$tran
    }catch{}
    if(!$call){write (gwmi win32_process -f "handle=${pid}").commandline}
    if(!(test-path env:oracle_sid)){throw 'Нет ORACLE_SID'}
#    $env:path="C:\oracle\product\10.2.0\db_1\BIN;${env:path}"
    $tnsp=gcm tnsping.exe
    $rman=gcm rman.exe
    [void][reflection.assembly]::loadwithpartialname('Oracle.DataAccess')
    dispose-after($oc=new-object oracle.dataaccess.client.oracleconnection){
      $oc.connectionstring='user id=/;dba privilege=sysdba'
      $oc.open()
      dispose-after($cm=$oc.createcommand()){
        $cm.commandtext=@'
select name,database_role role
from v$database
where database_role in('PRIMARY','PHYSICAL STANDBY')
'@
        dispose-after(
          $da=new-object oracle.dataaccess.client.oracledataadapter){
          $da.selectcommand=$cm
          dispose-after($dt=new-object data.datatable){
            if($da.fill($dt) -ne 1){throw "Чтение из '$($cm.commandtext)'"}
            $r1=$dt.rows[0]
            $props.db=$r1.name.tolower()
            $props.role=$r1.role.tolower()
            if($props.role -ne 'primary'){$props.role='standby'}
          }
<#        $rd=$cm.executereader('singleresult,singlerow')
        try{
          if(!$rd.read()){throw "Чтение из '$($cm.commandtext)'"}
          $props.db=$rd['name'].tolower()
          $props.role=$rd['database_role'].tolower()
          if($props.role -ne 'primary'){$props.role='standby'}
        }finally{if(!$rd.isclosed){$rd.close()}}#>
        }
      }
    }
    $db,$role=$props['db','role']
    $nsn="$('standby','primary'|?{$_ -ne $role})_ekr"
    $out=tnsping.exe $nsn
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'tnsping.exe ${nsn}'"
    }
    $NWLN=[environment]::newline
    for(;;){
      $out|
      ?{$_ -match '\(\s*host\s*=\s*(\w(?:-*\w)*(?:\.\w(?:-*\w)*)*)\s*\)'}|
      %{break}
      throw ($out|?{$_.trim() -ne [string]::empty}) -join $NWLN
    }
    $svr=[net.dns]::gethostentry($matches[1]).hostname
    $bkp="\\${svr}\archivedlogs\backup\%d-${role}-%T-%t-%s-"
    if($role -eq 'primary'){
      $scr=@"
configure retention policy to recovery window of 3 days;
configure controlfile autobackup off;
crosscheck backup;
crosscheck copy;
run{
  set command id to 'nightly full ${db} ${role} db backup';
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
delete noprompt expired backup;
delete noprompt expired copy;
delete noprompt obsolete;
"@
    }else{
      $scr=@"
configure controlfile autobackup off;
crosscheck backup;
crosscheck copy;
run{
  set command id to 'nightly full ${db} ${role} db backup';
  backup as compressed backupset check logical
    database
      format '${bkp}db.bkp'
    plus archivelog
      format '${bkp}arc.bkp';
}
delete noprompt expired backup;
delete noprompt expired copy;
delete noprompt obsolete recovery window of 2 days;
#delete noprompt obsolete redundancy=2;
"@
    }
<#    $rman="backup_${db}_${role}"
    $log=split-path $log|join-path -ch "${rman}_${dt}.log"
    $rman=split-path $sn|join-path -ch "${rman}.rman"
    [io.file]::writealltext($rman,$scr,[text.encoding]::getencoding(1251))
    $rman="rman.exe target / nocatalog ``@""$rman"""
    &([scriptblock]::create($rman))|oh#>
    if($props.tran){stop-transcript|write;$props.tran=$null}
    $log="$(split-path $log)\backup_${db}_${role}_${dt}.log"
    try{
      start-transcript $log -f -outv tran|write
      $props.tran=$tran
    }catch{}
    write $scr
    $scr="set echo on;${NWLN}${scr}${NWLN}exit${NWLN}"
    $scr|&$rman target / nocatalog|write
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'rman.exe'"
    }
    if(!$call){write $sw.elapsed;exit 0}
  }catch{
    if($call){throw}else{$_|out-string|write-warning|write;exit 1}
  }finally{
    if($props.tran){stop-transcript|write}
  }
}
