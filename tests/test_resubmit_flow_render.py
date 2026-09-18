"""「调整参数重新提交」：只新建作业，原作业状态不被改动。"""
import unittest

import app as app_module


class ResubmitFlowRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_poll_is_pinned_to_the_job_it_started_for(self):
        """切换作业后，上一轮在途响应不能把旧作业画回来、也不能停掉新作业轮询。"""
        html = self._html()

        self.assertIn("const jobId = state.jobId;", html)
        self.assertIn("if (state.jobId !== jobId) return;", html)
        self.assertIn(
            "if (state.jobId === jobId && json.job.status !== 'running') stopPolling();",
            html,
        )

    def test_confirm_says_origin_job_is_not_touched(self):
        html = self._html()

        self.assertIn("function originJobNotice()", html)
        self.assertIn("originJobNotice() +", html)
        self.assertIn("状态与记录不会被改动", html)
        self.assertIn("不会取消或改动它", html)
        # 参数回放时记住来源作业的状态，弹窗才能区分"还在跑"和"已结束"。
        self.assertIn("state.wizardOriginJobStatus = job ? job.status : null;", html)

    def test_empty_password_blocks_submit(self):
        """快照不存口令，回放后为空时必须先补填，别建出必然失败的作业。"""
        html = self._html()

        self.assertIn("口令不进提交快照，重新提交时不会自动带回来", html)
        self.assertIn("if (!($('#profile-select') && $('#profile-select').value)) {", html)


if __name__ == "__main__":
    unittest.main()
