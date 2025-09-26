import streamlit as st
import pandas as pd
import time

class NotificationManager:
    """
    Manages tracking and setting session state flags for new order notifications.
    It detects new sales and sets flags that the main script can use to display
    persistent visual feedback like banners and a generic rain effect.
    """
    def __init__(self, session_state_key="last_seen_order_ids"):
        """
        Initializes the NotificationManager.
        
        Args:
            session_state_key (str): The key used to store seen order IDs in st.session_state.
        """
        self.state_key = session_state_key
        if self.state_key not in st.session_state:
            st.session_state[self.state_key] = set()

    def _get_previous_order_ids(self) -> set:
        """Gets the set of previously seen order IDs from the session state."""
        return st.session_state.get(self.state_key, set())

    def _update_order_ids(self, new_ids: set):
        """Updates the session state with the new set of order IDs."""
        st.session_state[self.state_key] = new_ids

    def check_for_new_sales(self, order_details: list):
        """
        Checks for new sales and sets session state flags to trigger UI effects.
        
        Args:
            order_details (list): A list of dictionary objects, where each object represents
                                  an order and must contain 'id', 'marketer', 'total_revenue', 
                                  and 'products' keys.
        """
        if not order_details:
            return

        previous_ids = self._get_previous_order_ids()
        current_ids = {order['id'] for order in order_details}
        
        new_order_ids = current_ids - previous_ids

        if new_order_ids:
            new_sales_messages = []
            for new_id in new_order_ids:
                order = next((o for o in order_details if o['id'] == new_id), None)
                if order:
                    products_str = ", ".join(order['products'])
                    revenue_str = f"${order['total_revenue']:.2f}"
                    message = (
                        f"🎉 **New Sale for {order['marketer']}!** "
                        f"Products: *{products_str}*. "
                        f"Total Revenue: **{revenue_str}**"
                    )
                    new_sales_messages.append(message)
            
            if new_sales_messages:
                st.session_state.banner_notification = " \n\n ".join(new_sales_messages)
                st.session_state.show_celebration = True
                st.session_state.celebration_start_time = time.time()

        self._update_order_ids(current_ids)
