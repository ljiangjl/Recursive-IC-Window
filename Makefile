##
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
##

#The very simple makefile for the example UDOs. This is the "standalone" version. Rename this file to "Makefile"
#in order to enable it.

#The SciDB source must be present in order to build. It is specified
#As the SCIDB_SOURCE_DIR variable.

#Example: make SCIDB_SOURCE_DIR=/home/user/workspace/scidb_trunk

#The scidb-boost-devel package also needs to be installed:
BOOST_LOCATION=/opt/scidb/14.12/3rdparty/boost


#-pedantic
CFLAGS=-pedantic -W -Wextra -Wall -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -fPIC -D__STDC_FORMAT_MACROS -Wno-system-headers -isystem -O2 -g -DNDEBUG -ggdb3  -D__STDC_LIMIT_MACROS
INC=-I. -DPROJECT_ROOT="\"$(SCIDB_SOURCE_DIR)\"" -I"$(SCIDB_SOURCE_DIR)/include" -I"$(SCIDB_SOURCE_DIR)/src" -I"$(BOOST_LOCATION)/include"
LIBS=-L"$(SCIDB_SOURCE_DIR)/lib" -shared -Wl,-soname,libexample_udos.so -lm -L.

all:
	@if test ! -d "$(SCIDB_SOURCE_DIR)"; then echo  "Error. Try:\n\nmake SCIDB_SOURCE_DIR=<PATH TO SCIDB TRUNK>"; exit 1; fi 

	$(CXX) $(CFLAGS) $(INC) -o plugin.cpp.o -c plugin.cpp
	$(CXX) $(CFLAGS) $(INC) -o RicWindowArray.cpp.o -c RicWindowArray.cpp
	$(CXX) $(CFLAGS) $(INC) -o LogicalRicWindow.cpp.o -c LogicalRicWindow.cpp
	$(CXX) $(CFLAGS) $(INC) -o PhysicalRicWindow.cpp.o -c PhysicalRicWindow.cpp

	$(CXX) $(CFLAGS) $(INC) -o IcWindowArray.cpp.o -c IcWindowArray.cpp
	$(CXX) $(CFLAGS) $(INC) -o LogicalIcWindow.cpp.o -c LogicalIcWindow.cpp
	$(CXX) $(CFLAGS) $(INC) -o PhysicalIcWindow.cpp.o -c PhysicalIcWindow.cpp



	$(CXX) $(CFLAGS) $(INC) -o libicwindow.so \
							   plugin.cpp.o \
							   IcWindowArray.cpp.o \
							   LogicalIcWindow.cpp.o \
							   PhysicalIcWindow.cpp.o \
							   RicWindowArray.cpp.o \
							   LogicalRicWindow.cpp.o \
							   PhysicalRicWindow.cpp.o \
	                           $(LIBS)

clean:
	rm -f *.o *.so
