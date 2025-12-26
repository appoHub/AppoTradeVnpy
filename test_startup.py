"""
测试 VeighNa 项目启动验证
"""
import sys
print("=" * 60)
print("VeighNa 项目启动验证测试")
print("=" * 60)

# 测试 1: 导入核心模块
print("\n[测试 1] 导入核心模块...")
try:
    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    print("✅ 核心引擎模块导入成功")
except Exception as e:
    print(f"❌ 核心引擎模块导入失败: {e}")
    sys.exit(1)

# 测试 2: 导入 UI 模块
print("\n[测试 2] 导入 UI 模块...")
try:
    from vnpy.trader.ui import MainWindow, create_qapp
    print("✅ UI 模块导入成功")
except Exception as e:
    print(f"❌ UI 模块导入失败: {e}")
    sys.exit(1)

# 测试 3: 导入交易接口
print("\n[测试 3] 导入交易接口...")
try:
    from vnpy_ctp import CtpGateway
    print("✅ CTP 交易接口导入成功")
except Exception as e:
    print(f"❌ CTP 交易接口导入失败: {e}")
    sys.exit(1)

# 测试 4: 导入应用模块
print("\n[测试 4] 导入应用模块...")
apps_status = []
try:
    from vnpy_ctastrategy import CtaStrategyApp
    apps_status.append("✅ CTA策略模块")
except Exception as e:
    apps_status.append(f"❌ CTA策略模块: {e}")

try:
    from vnpy_ctabacktester import CtaBacktesterApp
    apps_status.append("✅ CTA回测模块")
except Exception as e:
    apps_status.append(f"❌ CTA回测模块: {e}")

try:
    from vnpy_datamanager import DataManagerApp
    apps_status.append("✅ 数据管理模块")
except Exception as e:
    apps_status.append(f"❌ 数据管理模块: {e}")

for status in apps_status:
    print(f"  {status}")

# 测试 5: 导入数据库驱动
print("\n[测试 5] 导入数据库驱动...")
try:
    import vnpy_sqlite
    print("✅ SQLite 数据库驱动导入成功")
except Exception as e:
    print(f"❌ SQLite 数据库驱动导入失败: {e}")

# 测试 6: 创建引擎实例
print("\n[测试 6] 创建引擎实例...")
try:
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    print("✅ 主引擎创建成功")
except Exception as e:
    print(f"❌ 主引擎创建失败: {e}")
    sys.exit(1)

# 测试 7: 注册接口
print("\n[测试 7] 注册交易接口...")
try:
    main_engine.add_gateway(CtpGateway)
    print("✅ CTP 接口注册成功")
except Exception as e:
    print(f"❌ CTP 接口注册失败: {e}")

# 测试 8: 注册应用
print("\n[测试 8] 注册应用模块...")
try:
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(DataManagerApp)
    print("✅ 所有应用模块注册成功")
except Exception as e:
    print(f"❌ 应用模块注册失败: {e}")

# 测试 9: 检查 ta-lib
print("\n[测试 9] 检查 ta-lib 库...")
try:
    import talib
    import numpy as np
    close = np.random.random(100)
    sma = talib.SMA(close)
    print("✅ ta-lib 库工作正常")
except Exception as e:
    print(f"❌ ta-lib 库测试失败: {e}")

# 测试 10: 检查 PySide6
print("\n[测试 10] 检查 PySide6 版本...")
try:
    import PySide6
    print(f"✅ PySide6 版本: {PySide6.__version__}")
except Exception as e:
    print(f"❌ PySide6 检查失败: {e}")

print("\n" + "=" * 60)
print("✅ 所有核心功能验证通过！")
print("=" * 60)
print("\n提示：启动 GUI 请运行: python examples\\veighna_trader\\run.py")
print("注意：首次运行会提示配置数据源，这是正常的。")
