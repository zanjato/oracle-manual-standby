#requires -version 2
<#set-executionpolicy remotesigned localmachine -f
get-acl (join-path $env:windir\system32\tasks manual_standby_recovery.job)|
  set-acl (join-path $env:windir\system32\tasks backup_oracle.job)
powershell.exe -nol -nop -non -f backup_oracle.ps1[ -call]#>
param([switch]$call)
set-strictmode -vers latest
&{
  function chkpars{
    param([parameter(mandatory=$true)][validatenotnull()]
          [management.automation.commandinfo]$ci,
          [parameter(mandatory=$true)][validatenotnull()]
          [collections.generic.dictionary[string,object]]$bndpars)
    $ci.parameters.getenumerator()|?{!$_.value.switchparameter}|
    %{gv $_.key -ea silentlycontinue}|?{!$bndpars.containskey($_.name)}|
    %{throw "Функция '$($ci.name)' вызвана без параметра '$($_.name)'"}
  }
  function dispose-after{[cmdletbinding()]
    param([validatenotnull()][object]$obj,[validatenotnull()][scriptblock]$sb)
    chkpars $myinvocation.mycommand $psboundparameters
    try{&$sb}
    finally{
      if($obj -is [idisposable] -or $obj -as [idisposable]){
        [void][idisposable].getmethod('Dispose').invoke($obj,$null)
      }
    }
  }
  function log{[cmdletbinding()]
    param([parameter(valuefrompipeline=$true)]
          [validatenotnullorempty()][string]$log,[switch]$err)
    begin{$f=$myinvocation.mycommand}
    process{
      chkpars $f $psboundparameters
      $log|%{$_.replace($LNW,$NL.str).split($NL.ach,$f.REE)}|? $f.CE|
      %{$i=0}{"$(&$f.LLB[$i]) $(if($err){'!!'}else{'--'}) $_";$i=1}|
      write-host
    }
  }
  function mk_oc{[cmdletbinding()]param([validatenotnullorempty()][string]$cs)
    chkpars $myinvocation.mycommand $psboundparameters
    $oc=new-object oracle.dataaccess.client.oracleconnection $cs
    $oc.open()
    $oc
  }
  $erroractionpreference='stop'
  gcm log|add-member @{REE=[stringsplitoptions]'removeemptyentries'
                       CE={!!$_ -and $_.trim() -ne [string]::empty}
                       LLB={date -f 'yyyy-MM-dd HH:mm:ss.fffffff'},{' '*27}}
  try{
    $sw=[diagnostics.stopwatch]::startnew()
    $props=@{tran=$null}
    $dt=date
    $dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
    $sn=$myinvocation.scriptname
    $log=[io.path]::getfilenamewithoutextension($sn)
    $log="$(split-path $sn)\logs\${log}_${dt}.log"
    $LNW=[environment]::newline
    $NL=@{str="`n";ach=[char[]]"`n"}
    $REE=[stringsplitoptions]::removeemptyentries
    $lnbg={date -f 'yyyy-MM-dd HH:mm:ss.fffffff'},{' '*27}
    try{start-transcript $log -f -outv tran|log;$props.tran=$tran}catch{}
    if(!$call){log (gwmi win32_process -f "handle=${pid}").commandline}
    if(!(test-path env:oracle_sid)){throw 'Нет ORACLE_SID'}
#    $env:path="C:\oracle\product\10.2.0\db_1\BIN;${env:path}"
    $tnsp=gcm tnsping.exe
    $rman=gcm rman.exe
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
            $r0=$tbl.rows[0]
            $props.db=$r0.name.tolower()
            $props.role=$r0.role.tolower()
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
    $chkempty={!!$_ -and $_.trim() -ne [string]::empty}
    for(;;){
      $out|
      ?{$_ -match '\(\s*host\s*=\s*(\w(?:-*\w)*(?:\.\w(?:-*\w)*)*)\s*\)'}|
      %{break}
      throw ($out|? $chkempty) -join $LNW
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
    if($props.tran){stop-transcript|log;$props.tran=$null}
    $log="$(split-path $log)\backup_${db}_${role}_${dt}.log"
    try{start-transcript $log -f -outv tran|log;$props.tran=$tran}catch{}
    log $scr
    $scr="set echo on;${LNW}${scr}${LNW}exit${LNW}"
    $scr|&$rman target / nocatalog|? $chkempty|log
    if($lastexitcode -ne 0){
      throw "Ошибка '${lastexitcode}' выполнения 'rman.exe'"
    }
    if(!$call){log "Затрачено '$($sw.elapsed)'";exit 0}
  }catch{if($call){throw}else{$_|out-string|log -err;exit 1}}
   finally{if($props.tran){stop-transcript >$null}}
}