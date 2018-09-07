EVENT_FIELDS = ['attendance_count', 'manual_attendance_count', 'comment_count',
                'plain_text_description', 'short_link']
GROUP_FIELDS = ['topics', 'plain_text_description', 'membership_dues']
GROUP_KWARGS = {
    'zip': 94305,
    'radius': 35,
    'category': '30,31',
    'order': 'newest',
    'fields': ','.join(GROUP_FIELDS)
}

EVENT_KWARGS = {
    'status': 'past',
    'fields': ','.join(EVENT_FIELDS)
}