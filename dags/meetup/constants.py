EVENT_FIELDS = ['attendance_count', 'manual_attendance_count', 'comment_count',
                'plain_text_description', 'short_link']
GROUP_FIELDS = ['topics', 'plain_text_description', 'membership_dues']
DATING_TOPIC_IDS = {
    'Dating and Relationships': '20246',
    'Dating for Mature Cool Singles': '124696',
    'Online Dating': '40347',
    'Lesbian Dating': '16766',
    'Dating over 45\'s': '19639',
    'Interracial Dating': '25504',
    'Dating over 35': '20662',
    'Dating Again': '20814',
    'Single and Dating Again': '30376',
    'Dating & Relationships / Romance': '34247'
}
GROUP_KWARGS = {
    'zip': 94305,
    'radius': 35,
    'category': '30,31',
    'order': 'newest',
    'fields': ','.join(GROUP_FIELDS),
    'topic_id': ','.join([t for t in DATING_TOPIC_IDS.values()])
}

EVENT_KWARGS = {
    'status': 'past',
    'fields': ','.join(EVENT_FIELDS)
}

