import subprocess
import time
import os
import signal
import psutil
from pathlib import Path


class ChromeDebugController:

    def __init__(self, port=9222, user_data_dir=None):
        self.edge_path = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
        self.port = port

        if user_data_dir is None:
            self.user_data_dir = r'C:\专高六\IDEAFIle\offline_v2\stream-bda-data-dev\edge_data'
        else:
            self.user_data_dir = user_data_dir

        self.process = None
        self.pid = None

    def start(self):
        try:
            if not Path(self.edge_path).exists():
                raise FileNotFoundError(f"未找到Edge浏览器：{self.edge_path}")

            data_dir = Path(self.user_data_dir)
            data_dir.mkdir(parents=True, exist_ok=True)
            test_file = data_dir / "test_write.txt"
            with open(test_file, "w", encoding="utf-8") as f:
                f.write("test")
            test_file.unlink(missing_ok=True)

            command = [
                self.edge_path,
                f"--remote-debugging-port={self.port}",
                f"--user-data-dir={str(data_dir)}",
                "--no-first-run",
                "--no-default-browser-check",
                "--start-maximized",
                "about:blank"
            ]

            self.process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )

            time.sleep(5)

            if self.process.poll() is None:
                self.pid = self.process.pid
                print(f"✅ Edge 已启动 | 端口: {self.port} | PID: {self.pid}")
                print(f"用户数据目录: {data_dir}")
                return True
            else:
                error = self.process.stderr.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Edge 启动失败: {error}")

        except PermissionError:
            print(f"❌ 权限错误：无法写入目录 {data_dir}")
            return False
        except Exception as e:
            print(f"❌ 启动失败: {str(e)}")
            return False

    def close(self):
        if self.process is None:
            print("⚠️ 没有正在运行的 Edge 进程")
            return False

        try:
            if os.name == 'nt' and self.pid:
                os.kill(self.pid, signal.CTRL_C_EVENT)
                time.sleep(2)
                if self.process.poll() is None:
                    os.kill(self.pid, signal.SIGTERM)

            self.process.wait(timeout=10)
            print(f"✅ Edge 已关闭 | PID: {self.pid}")
            self.process = None
            self.pid = None
            return True

        except psutil.NoSuchProcess:
            print(f"⚠️ 进程已结束 | PID: {self.pid}")
            self.process = None
            self.pid = None
            return True
        except Exception as e:
            print(f"❌ 关闭失败: {str(e)}")
            return False

    def is_running(self):
        if self.process is None:
            return False

        if self.process.poll() is None:
            return True

        try:
            psutil.Process(self.pid)
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def connect_drissionpage(self):
        if not self.is_running():
            print("⚠️ Edge 未运行，请先启动")
            return None

        try:
            from DrissionPage import ChromiumPage
            # 直接使用ChromiumPage连接调试端口
            page = ChromiumPage(f'http://localhost:{self.port}')
            print("✅ 成功连接到 Edge")
            return page
        except ImportError:
            print("❌ 未安装 DrissionPage，请执行：pip install DrissionPage==4.1.0.18")
            return None
        except Exception as e:
            print(f"❌ 连接失败: {str(e)}")
            return None