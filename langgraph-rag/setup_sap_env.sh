#!/bin/bash
# SAP NWRFC SDK 환경변수 설정 스크립트

# SAP NWRFC SDK 경로
export SAPNWRFC_HOME="$HOME/sap/nwrfcsdk"

# Library 경로 설정 (macOS)
export DYLD_LIBRARY_PATH="$SAPNWRFC_HOME/lib:$DYLD_LIBRARY_PATH"
export LD_LIBRARY_PATH="$SAPNWRFC_HOME/lib:$LD_LIBRARY_PATH"

# PATH 설정
export PATH="$SAPNWRFC_HOME/bin:$PATH"

echo "✅ SAP NWRFC SDK 환경변수 설정 완료"
echo "   SAPNWRFC_HOME: $SAPNWRFC_HOME"
echo "   DYLD_LIBRARY_PATH: $DYLD_LIBRARY_PATH"

# .zshrc 또는 .bash_profile에 추가할 내용 출력
echo ""
echo "📌 아래 내용을 ~/.zshrc 또는 ~/.bash_profile에 추가하세요:"
echo ""
echo "# SAP NWRFC SDK"
echo 'export SAPNWRFC_HOME="$HOME/sap/nwrfcsdk"'
echo 'export DYLD_LIBRARY_PATH="$SAPNWRFC_HOME/lib:$DYLD_LIBRARY_PATH"'
echo 'export PATH="$SAPNWRFC_HOME/bin:$PATH"'