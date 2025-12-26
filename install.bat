@ECHO OFF
SET python=%1
SET pypi_index=%2
IF     %python%""     == "" SET python=python
IF     %pypi_index%"" == "" SET pypi_index=https://pypi.vnpy.com
IF NOT %pypi_index%"" == "" SET pypi_index=--index-url %pypi_index%
@ECHO ON

:: Upgrade pip & wheel
%python% -m pip install --upgrade pip wheel %pypi_index%

::Install prebuild wheel from local pack directory
%python% -m pip install pack/ta_lib-0.6.8-cp314-cp314-win_amd64.whl

:: Install VeighNa
%python% -m pip install .