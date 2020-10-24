from scb.etl.geo_utils import approx_dist


def test_appro_dist():
    dist = approx_dist(42.2077950049541, -83.3562970161438, 32.8140177, -96.9488945)
    assert dist > 0
