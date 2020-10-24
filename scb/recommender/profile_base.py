from collections import defaultdict
from functools import reduce

import bokeh.models
import numpy as np
import pandas as pd
from bokeh.plotting import gmap


def gen_map(profile, date_filter, api_key, zoom=8):
    """
    Returns a bokeh plot
    :param profile:
    :param date_filter:
    :param api_key:
    :param zoom:
    :return:
    """
    map_options = bokeh.models.GMapOptions(lat=profile.u_latitude, lng=profile.u_longitude, map_type="roadmap",
                                           zoom=zoom)

    plt = gmap(api_key, map_options, title="Checkings")
    # pandas here is bit of an overkill but it will to the transposition directly so I am using it
    df = pd.DataFrame([c.asDict() for c in profile.chks])
    source = bokeh.models.ColumnDataSource(
        data=df[["latitude", "longitude"]])
    plt.circle(x="longitude", y="latitude", size=5, fill_color="blue", fill_alpha=0.8, source=source)
    plt.line(x="longitude", y="latitude", color="red", alpha=0.8, line_width=4, source=source)
    return plt


def user_rank_from_friends(profile):
    """
    Given a profiles returns a dictionary of {venue_id: score}
    :param profile:
    :return: dict
    """

    def get_venue_struct(target_profile):
        venues = {}
        f_venue = defaultdict(list)
        for checkin in target_profile.chks:
            venues[checkin.venue_id] = (checkin.rating, checkin.total_venue_chk)

        if 'friends_profile' not in target_profile.asDict():
            return venues, f_venue

        if target_profile.friends_profile is None:
            return venues, f_venue

        for friend_profile in target_profile.friends_profile:
            friend_venue, _ = get_venue_struct(friend_profile)

            for k, v in friend_venue.items():
                f_venue[k].append(v)

        for friend_venue, rating_set in f_venue.items():
            rating = list(reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), rating_set))
            rating[0] = rating[0] / len(rating_set)
            f_venue[friend_venue] = rating

        return venues, f_venue

    def rank(profile_venue, f_venue):
        score_data = {}
        for venue_id, venue_data in f_venue.items():
            friends_rank = venue_data[0]  # consider total checkins
            if venue_id not in profile_venue:
                score_data[venue_id] = friends_rank  # Consider total checkins
                continue

            user_rank = profile_venue[venue_id][0]
            if user_rank >= friends_rank:
                # Naively, this means user really liked it, we just add it even more, this can be a separate learned function
                score_data[venue_id] = user_rank + friends_rank  # Consider total checkins
            else:
                # If my friend likes it more than me, they can pull my score up. It is similar weight
                score_data[venue_id] = (user_rank + friends_rank) / 2

        rank_data = {k: v for k, v in sorted(score_data.items(), key=lambda item: item[1], reverse=True)}

        return rank_data

    venue, friends_venue = get_venue_struct(profile)

    ranked = rank(venue, friends_venue)
    return ranked


def friends_closeness(profile):
    """
    Given a profile who are the closed friends by similar ranked places
    :param profile:
    :return:
    """

    def get_venue_struct(target_profile):
        profile_venues = {}
        for checkin in target_profile.chks:
            profile_venues[checkin.venue_id] = checkin  # (checkin.created_at, checkin.rating, checkin.total_venue_chk)
        return profile_venues

    def get_score(chk, profile_venues):
        return 1 - abs(chk.rating - profile_venues[chk.venue_id].rating)

    venues = get_venue_struct(profile)
    friends = {}
    for f in profile.friends_profile:
        scores = [get_score(chk, venues) for chk in f.chks if chk.venue_id in venues]
        friends[f.user_id] = sum(scores)
    return {k: v for k, v in sorted(friends.items(), key=lambda item: item[1], reverse=True)}


def next_place(profile, required_datetime):
    """
    Given a profile and datetime object returns the next most likely places the user will go sorted by likelyhood
    :param profile:
    :param required_datetime: datetime object
    :return: dict(venue_id, likelyhood: double)
    """

    def get_venue_struct(target_profile):
        p_venues = {}
        for checkin in target_profile.chks:
            p_venues[checkin.venue_id] = (checkin.created_at, checkin.rating, checkin.total_venue_chk)
        return p_venues

    def rank_venue(unsorted_venues, required_time):
        deltas = np.array([(venue_data[0] - required_time).days for venue, venue_data in unsorted_venues.items()])
        # We only care about days, in the future we can care about the actual hour user is travelling
        e = np.exp(deltas / 3000)  # Divided by constant = 3000 to consider data freshness

        ck_count = np.array([venue_data[1] for venue_data in unsorted_venues.values()])
        r = e * ck_count
        r /= np.max(r, axis=0)
        sorted_idx = np.argsort(r, )[::-1]
        keys = np.array([k for k in unsorted_venues.keys()])
        scores = dict(zip(keys[sorted_idx], r[sorted_idx]))
        return scores

    venues = get_venue_struct(profile)
    return rank_venue(venues, required_datetime)
