<#function chkpars{
  param([parameter(mandatory=$true)][validatenotnull()]
        [management.automation.commandinfo]$ci,
        [parameter(mandatory=$true)][validatenotnull()]
        [collections.generic.dictionary[string,object]]$bp)
  $ci.parameters.getenumerator()|?{!$_.value.switchparameter}|
  %{gv $_.key -ea 0}|?{!$bp.containskey($_.name)}|
  %{throw "Функция '$($ci.name)' вызвана без параметра '$($_.name)'"}
  #chkpars $myinvocation.mycommand $psboundparameters
}#>
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
  $rui=$host.ui.rawui
  if(!$rui.windowsize -or $rui.windowsize.width -le $bsw){
    $bsw,$bs.width=($bs=$rui.buffersize).width,$bsw
    $rui.buffersize=$bs
    $my.bsw=$bsw
  }
}
function log{param([parameter(valuefrompipeline=$true)][string]$log,
                   [switch]$err)
  process{
    $log|? $my.CE|%{$_.replace($my.LNW,$my.NL.str).split($my.NL.ach,$my.REE)}|
    ? $my.CE|%{$i=0}{"$(&$my.LLB[$i]) $(if($err){'!!'}else{'--'}) $_";$i=1}|oh
  }
}
function mk_log{
  $dt=date
  $my.dt='{0}_{1:HHmm}' -f (($dt-[datetime]0).days%7+1),$dt
  $sn=split-path $myinvocation.scriptname -le
  $log=[io.path]::getfilenamewithoutextension($sn)
  $log="logs\${log}_$($my.dt).log"
  try{start-transcript $log -f|log;$my.tran=$true}catch{}
}
function dispose-after{
  param([validatenotnull()][object]$obj,[validatenotnull()][scriptblock]$sb)
  try{&$sb}
  finally{
    if($obj -as [idisposable]){
      [void][idisposable].getmethod('Dispose').invoke($obj,$null)
    }
  }
}
function mk_oc{param([validatenotnullorempty()][string]$cs)
  $oc=new-object oracle.dataaccess.client.oracleconnection $cs
  $oc.open()
  $oc
}
