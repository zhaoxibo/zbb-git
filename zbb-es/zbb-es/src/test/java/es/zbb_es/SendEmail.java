package es.zbb_es;

public class SendEmail {
	//静态内部类
	static class User{
		private static String name;
		private static String emailAddress;
		private static int i = 0;
		private User(String name,String emailAddress) {
			i++;
		}
	}

	public static void main(String[] args) {
		
		User u = new User("jack","m18725917011@163.com");
		System.out.println(u.name+" "+u.i);
		User u1 = new User("jack","m18725917011@163.com");
		System.out.println(u1.name+" "+u1.i);
		User u2 = new User("zbb","m18725917011@163.com");
		System.out.println(u2.name+" "+u2.i);
		User u3 = new User("zbb","m18725917011@163.com");
		System.out.println(u3.name+" "+u3.i);		
	}
	
	
}
