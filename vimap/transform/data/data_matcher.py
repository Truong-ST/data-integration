


class DataMatcher:
    def __init__(self):
        pass
    
    
    def matching(collection, matching_dict):
        """
        return: command string 
        """
        for record in collection:
            if matching_dict['id'] == record['id']:
                return 'update'
        return 'insert'