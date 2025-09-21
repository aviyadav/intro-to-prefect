from pipeline.main import clean_stats

def test_clean_stats_data():
    sample_input = {
        "sold": 5,
        "Sold": 3,
        "pending": 2,
        "Pending": 1,
        "available": 10,
        "Available": 5,
        "unavailable": 0,
        "Unavailable": 4
    }

    expected_output = {
        "sold": 8,          # 5 + 3
        "pending": 3,       # 2 + 1
        "available": 15,    # 10 + 5
        "unavailable": 4    # 0 + 4
    }

    assert clean_stats(sample_input) == expected_output