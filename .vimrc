au BufReadPost * if line("'\"") > 0|if line("'\"") <= line("$")|exe("norm '\"")|else|exe "norm $"|endif|endif

if has("syntax")
  syntax on
endif

set nocompatible
set ruler
set showmode
set showmatch
set tabstop=3 
set autoindent
set wrapmargin=5
set nojoinspaces
set ignorecase
set smartcase
set scrolloff=0
set nowrapscan
set hlsearch
set linebreak
set breakat-=_
set showbreak="-->"
set display=lastline
set laststatus=0
set shiftround
set noincsearch
set shiftwidth=3
set cindent
set backspace=indent,eol,start
set formatoptions=tq
set backupext=.bak
set spelllang=en_us

filetype on

ab teh the
ab hte the
ab fro for
ab Chian China
ab waht what
ab inot into
ab tanh than
ab taht that
ab donw down
ab aks ask
ab sht sth
ab oen one
ab adn and
ab ned end
ab ahppen happen


