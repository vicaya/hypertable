class String
  def blank?
    strip.empty?
  end
end

class NilClass
  def blank?
    true
  end
end
