

class RequestFields:
    def __init__(
        self,
        _identity_items : list[str], 
        _static_fields : list[str],
        _calc_fields : list[str]
    ):
        self.identity_items : list[str] = _identity_items 
        self.static_fields : list[str] = _static_fields
        self.calc_fields : list[str] = _calc_fields

    def get_identity_items(self) -> list[str]:
        return self.identity_items
    
    def get_all_fields(self) -> list[str]:
        return self.static_fields + self.calc_fields
    
    def get_static_fields(self) -> list[str]:
        return  self.static_fields
    
    def get_calc_fields(self) -> list[str]:
        return  self.calc_fields