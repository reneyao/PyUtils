import requests
import json


class DingTalkBot:
    def __init__(self, access_token):
        """
        初始化 DingTalkBot 类，传入 access_token 来构造 webhook_url
        :param access_token: 钉钉机器人 access_token
        """
        self.webhook_url = (
            f"https://oapi.dingtalk.com/robot/send?access_token={access_token}"
        )

    def send_text(self, content):
        """发送文本消息"""
        message = {"msgtype": "text", "text": {"content": content}}
        return self._send_message(message)

    def send_markdown(self, title, text):
        """发送Markdown格式的消息"""
        message = {"msgtype": "markdown", "markdown": {"title": title, "text": text}}
        return self._send_message(message)

    def _send_message(self, message):
        """发送消息的私有方法"""
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(message),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()  # 如果响应码不是 200，将抛出异常
            return response.json()  # 返回响应的JSON内容
        except requests.exceptions.RequestException as e:
            print(f"发送消息失败: {e}")
            return None
