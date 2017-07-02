# .bashrc
# User specific aliases and functions

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

export THESIS=/Users/chenxu/Documents/Latex/mythesis/mythesis
export CFA=/Users/chenxu/Documents/Latex/CFA_FORMULA
export RESUME=/Users/chenxu/Documents/Latex/resume/test/new_moderncv/moderncv

#export ROOT_VERSION="5.24.00b"
#export ROOTSYS=$I3_PORTS/root-v$ROOT_VERSION
#if [ ! -d $ROOTSYS ]
#then
#	unset ROOTSYS
#fi


#export ICESIM_VERSION="Trunk-0217"
#export ICESIM_DIR=/Users/chenxu/Softwares/IceSim-$ICESIM_VERSION
#export COAST_DIR=/Users/chenxu/Softwares/coast-v3r3
#export COAST_USER_LIB=/Users/chenxu/Softwares/coast-interface-v3r2/Histogram
#export ROOTSYS=/Users/chenxu/Softwares/root_v5.34.03/build


#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$COAST_DIR/lib:$COAST_USER_LIB:$PYTHONDIR/lib:$ROOTSYS/lib
#export PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH


#export PATH=$ROOTSYS/bin:$ROOTSYS/lib:$PATH:$I3_PORTS/qt-4.6.0:$ROOTSYS/bin
#alias qmake=$I3_PORTS/qt-4.6.0/bin/qmake
#alias designer=$I3_PORTS/qt-4.6.0/bin/designer

alias bartol='ssh -X www.bartol.udel.edu'
alias icetviz='ssh -X icetviz.bartol.udel.edu'
alias asterix='ssh -X asterix.bartol.udel.edu'
alias pub='ssh -X pub.icecube.wisc.edu'
alias mitbbs='luit -encoding gbk ssh -1 deepthroat@mitbbs.com'
alias ubuntu='ssh -X 128.175.178.182'


#MAIL=/var/spool/mail/chen && export MAIL

export SVN_EDITOR=vim
export CLICOLOR=1

export PS1="\e[1;30m[\u@\h \W]\$ \e[m"

export LD_LIBRARY_PATH=/opt/local/lib:$LD_LIBRARY_PATH

##
# Your previous /Users/chenxu/.bash_profile file was backed up as /Users/chenxu/.bash_profile.macports-saved_2011-12-23_at_16:48:38
##

# MacPorts Installer addition on 2011-12-23_at_16:48:38: adding an appropriate PATH variable for use with MacPorts.
export PATH=/opt/local/bin:/opt/local/sbin:$PATH
# Finished adapting your PATH environment variable for use with MacPorts.
##
# Your previous /Users/chenxu/.bash_profile file was backed up as /Users/chenxu/.bash_profile.macports-saved_2011-12-23_at_16:51:40
##

# MacPorts Installer addition on 2011-12-23_at_16:51:40: adding an appropriate PATH variable for use with MacPorts.
#export PATH=/opt/local/bin:/opt/local/sbin:$PATH
# Finished adapting your PATH environment variable for use with MacPorts.

#export TERM=xterm


#. /Users/chenxu/Softwares/Torch/torch/install/bin/torch-activate

export QUANDLFUTURES=/Users/chenxu/Softwares/Quandl/data
