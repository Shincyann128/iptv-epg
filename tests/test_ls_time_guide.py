from datetime import datetime

from scripts.taiwan_guide import TAIPEI, parse_ls


def test_ls_times_keep_real_midnight_and_omit_unannounced_dates():
    payload = b"""
    <span id="navMonthTitle">2026-11</span>
    <div class="time-list">
      <span class="viewDate">11/30</span>
      <ul><li><time>23:00</time><h2>Before gap</h2></li></ul>
      <span class="viewDate">12/01</span>
      <ul><li><time>00:00</time><h2>Untimed A</h2></li>
          <li><time>00:00</time><h2>Untimed B</h2></li></ul>
      <span class="viewDate">12/02</span>
      <ul><li><time>00:00</time><h2>Confirmed midnight</h2></li>
          <li><time>02:00</time><h2>Second confirmed</h2></li></ul>
    </div>
    """
    rows = parse_ls(payload)

    midnight = datetime(2026, 12, 2, 0, tzinfo=TAIPEI)
    two_am = datetime(2026, 12, 2, 2, tzinfo=TAIPEI)
    assert rows == [
        (datetime(2026, 11, 30, 23, tzinfo=TAIPEI), None, "Before gap"),
        (midnight, two_am, "Confirmed midnight"),
        (two_am, None, "Second confirmed"),
    ]
