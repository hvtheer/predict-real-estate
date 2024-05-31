import tkinter as tk

def welcome():
  """Hiển thị lời chào khi nút được nhấn."""
  label["text"] = "Chào mừng bạn đã sử dụng Tkinter!"

# Tạo cửa sổ chính
window = tk.Tk()
window.title("Chương trình Tkinter đơn giản")

# Tạo nhãn
label = tk.Label(window, text="Nhấp vào nút để chào mừng!", font=("Arial", 12))
label.pack()

# Tạo nút
btn = tk.Button(window, text="Nhấp vào tôi", command=welcome)
btn.pack()

# Chạy chương trình
window.mainloop()
