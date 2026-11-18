"""Regression checks for the saved five-second monitor ported onto beta.3."""

import io
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import unas_monitor as um


@pytest.fixture
def monitor():
    mon = um.UNASMonitor(mqtt_enabled=False)
    mon.mqtt = Mock()
    mon.mqtt.publish.return_value = SimpleNamespace(rc=0)
    return mon


@pytest.mark.parametrize("rates", [(4.46, 4.39), (None, None)])
def test_network_loop_uses_beta3_topics_and_five_second_interval(monitor, monkeypatch, rates):
    monitor.mqtt_enabled = True
    monkeypatch.setattr(monitor, "get_network_throughput", lambda: rates)
    sleep = Mock(side_effect=KeyboardInterrupt)
    monkeypatch.setattr(um.time, "sleep", sleep)
    with pytest.raises(KeyboardInterrupt):
        monitor.network_monitor_loop()
    sleep.assert_called_once_with(5)
    if rates[0] is None:
        monitor.mqtt.publish.assert_not_called()
    else:
        assert monitor.mqtt.publish.call_args_list == [
            ((f"{um.MQTT_SYSTEM}/network_rx", "4.46"), {"retain": True}),
            ((f"{um.MQTT_SYSTEM}/network_tx", "4.39"), {"retain": True}),
        ]


def test_network_loop_retries_after_a_failed_sample(monitor, monkeypatch, caplog):
    monitor.mqtt_enabled = True
    sample = Mock(side_effect=[OSError("counter unavailable"), (1.0, 2.0)])
    monkeypatch.setattr(monitor, "get_network_throughput", sample)
    monkeypatch.setattr(um.time, "sleep", Mock(side_effect=[None, KeyboardInterrupt]))
    with pytest.raises(KeyboardInterrupt):
        monitor.network_monitor_loop()
    assert sample.call_count == 2
    assert monitor.mqtt.publish.call_count == 2
    assert "Network monitoring error" in caplog.text


def test_run_starts_saved_daemon_thread_and_keeps_system_interval(monitor, monkeypatch):
    thread = Mock()
    factory = Mock(return_value=thread)
    monkeypatch.setattr(um.threading, "Thread", factory)
    monitor.collect_and_publish = Mock()
    sleep = Mock(side_effect=KeyboardInterrupt)
    monkeypatch.setattr(um.time, "sleep", sleep)
    with pytest.raises(KeyboardInterrupt):
        monitor.run()
    factory.assert_called_once_with(
        target=monitor.network_monitor_loop, name="unas-network-monitor", daemon=True
    )
    thread.start.assert_called_once_with()
    monitor.collect_and_publish.assert_called_once_with()
    sleep.assert_called_once_with(30)


@pytest.mark.parametrize("live", [False, True])
def test_main_loop_does_not_resample_network_but_headless_collection_does(
    monitor, monkeypatch, live
):
    monitor.mqtt_enabled = live
    sample = Mock(return_value=(2.0, 1.0))
    monkeypatch.setattr(monitor, "get_network_throughput", sample)
    monkeypatch.setattr(monitor, "get_cpu_usage", lambda: 5)
    monkeypatch.setattr(monitor, "get_disk_throughput", lambda: (1.0, 0.0))
    monkeypatch.setattr(monitor, "run_cmd", lambda *args: "version")
    files = {"/proc/meminfo": "MemTotal: 2048 kB\nMemAvailable: 1024 kB\n"}
    monkeypatch.setattr(um, "open", lambda path: io.StringIO(files.get(path, "0")), raising=False)
    metrics = monitor.get_system_metrics()
    if live:
        sample.assert_not_called()
        assert "network_rx" not in metrics
        assert "network_tx" not in metrics
    else:
        sample.assert_called_once_with()
        assert (metrics["network_rx"], metrics["network_tx"]) == (2.0, 1.0)


def test_failed_publish_is_not_cached_and_can_retry(monitor, caplog):
    monitor.mqtt.publish.side_effect = [SimpleNamespace(rc=4), SimpleNamespace(rc=0)]
    monitor.publish_system("network_rx", 1.0)
    assert not monitor._last_published
    monitor.publish_system("network_rx", 1.0)
    monitor.publish_system("network_rx", 1.0)
    assert monitor.mqtt.publish.call_count == 2
    assert "MQTT publish failed" in caplog.text


def test_reconnect_clears_cached_values_so_they_are_republished(monitor):
    monitor.publish_system("network_rx", 1.0)
    monitor._on_connect(None, None, None, 0, None)
    assert not monitor._last_published
    monitor.publish_system("network_rx", 1.0)
    assert monitor.mqtt.publish.call_count == 3  # includes availability


def test_concurrent_identical_publishes_are_serialized(monitor):
    start = threading.Barrier(4)

    def publish(topic, payload, retain=False):
        threading.Event().wait(0.02)  # let other callers contend for the cache lock
        return SimpleNamespace(rc=0)

    monitor.mqtt.publish.side_effect = publish

    def worker():
        start.wait(timeout=2)
        monitor.publish_system("network_rx", 1.0)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    monitor.mqtt.publish.assert_called_once_with(
        f"{um.MQTT_SYSTEM}/network_rx", "1.0", retain=True
    )
